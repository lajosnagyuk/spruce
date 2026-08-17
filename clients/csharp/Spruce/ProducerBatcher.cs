using System.Threading.Channels;

namespace Spruce;

public sealed record ProducerBatcherOptions(int MaxMessages = 256, int MaxBytes = 1 << 20, TimeSpan? MaxDelay = null, int QueueDepth = 4096);

public sealed class ProducerBatcher : IAsyncDisposable
{
    private abstract record Command;
    private sealed record Item(string Topic, byte[] Payload, PublishOptions Options, TaskCompletionSource<PublishResult> Completion, CancellationTokenRegistration Cancellation) : Command;
    private sealed record Barrier(TaskCompletionSource Completion) : Command;

    private readonly SpruceClient _client;
    private readonly ProducerBatcherOptions _options;
    private readonly Channel<Command> _queue;
    private readonly Task _worker;
    private readonly CancellationTokenSource _shutdown = new();
    private int _closed;

    public ProducerBatcher(SpruceClient client, ProducerBatcherOptions? options = null)
    {
        _client = client;
        options ??= new();
        if (options.MaxMessages is < 1 or > 4096) throw new ArgumentOutOfRangeException(nameof(options));
        if (options.MaxBytes is < 5 or > 16 << 20) throw new ArgumentOutOfRangeException(nameof(options));
        if (options.QueueDepth < 1) throw new ArgumentOutOfRangeException(nameof(options));
        _options = options with { MaxDelay = options.MaxDelay ?? TimeSpan.FromMicroseconds(250) };
        _queue = Channel.CreateBounded<Command>(new BoundedChannelOptions(options.QueueDepth) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });
        _worker = RunAsync();
    }

    /// <summary>
    /// Queues a publish. Cancellation before flush removes the item from the outgoing batch. Once a flush has
    /// started, cancellation only stops the caller waiting; the broker may already have accepted the message.
    /// Use the returned result, or producer idempotency on the unbatched retry API, when acceptance certainty matters.
    /// </summary>
    public async Task<PublishResult> PublishAsync(string topic, ReadOnlyMemory<byte> payload, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(topic)) throw new ArgumentException("Topic is required", nameof(topic));
        if (payload.Length > 1 << 20 || payload.Length + 4 > _options.MaxBytes) throw new ArgumentOutOfRangeException(nameof(payload));
        options ??= new();
        if (!string.IsNullOrEmpty(options.IdempotencyKey) || options.Ack is not (null or "" or "local")) throw new ArgumentException("Options are incompatible with batch publishing", nameof(options));
        var keyBytes = System.Text.Encoding.UTF8.GetByteCount(options.Key ?? "");
        if (keyBytes > 8 * 1024 || payload.Length + keyBytes + 6 > _options.MaxBytes) throw new ArgumentOutOfRangeException(nameof(options), "Key and payload exceed the configured batch size");
        if (Volatile.Read(ref _closed) != 0) throw new ObjectDisposedException(nameof(ProducerBatcher));
        var completion = new TaskCompletionSource<PublishResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registration = cancellationToken.Register(() => completion.TrySetCanceled(cancellationToken));
        try { await _queue.Writer.WriteAsync(new Item(topic, payload.ToArray(), options, completion, registration), cancellationToken); }
        catch { registration.Dispose(); throw; }
        return await completion.Task.WaitAsync(cancellationToken);
    }

    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (Volatile.Read(ref _closed) != 0) throw new ObjectDisposedException(nameof(ProducerBatcher));
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await _queue.Writer.WriteAsync(new Barrier(completion), cancellationToken);
        await completion.Task.WaitAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0) { await _worker; return; }
        _shutdown.CancelAfter(TimeSpan.FromSeconds(30));
        Exception? flushError = null;
        try
        {
            var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await _queue.Writer.WriteAsync(new Barrier(completion));
            try { await completion.Task; } catch (Exception ex) { flushError = ex; }
        }
        finally
        {
            _queue.Writer.TryComplete();
            try { await _worker; } catch when (flushError is not null) { }
            _shutdown.Dispose();
        }
        if (flushError is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(flushError).Throw();
    }

    private async Task RunAsync()
    {
        var pending = new List<Item>(_options.MaxMessages);
        await foreach (var command in _queue.Reader.ReadAllAsync())
        {
            if (command is Barrier barrier)
            {
                var error = await FlushPendingAsync(pending);
                if (error is null) barrier.Completion.TrySetResult(); else barrier.Completion.TrySetException(error);
                continue;
            }
            var item = (Item)command;
            if (pending.Count > 0 && (!Compatible(pending[0], item) || pending.Count >= _options.MaxMessages || Bytes(pending) + item.Payload.Length + 4 > _options.MaxBytes)) await FlushPendingAsync(pending);
            pending.Add(item);
            var deadline = DateTime.UtcNow + _options.MaxDelay!.Value;
            while (pending.Count < _options.MaxMessages && Bytes(pending) < _options.MaxBytes)
            {
                while (_queue.Reader.TryRead(out var next))
                {
                    if (next is Barrier nextBarrier) { var error = await FlushPendingAsync(pending); if (error is null) nextBarrier.Completion.TrySetResult(); else nextBarrier.Completion.TrySetException(error); goto flushed; }
                    var candidate = (Item)next;
                    if (!Compatible(pending[0], candidate) || Bytes(pending) + EntryBytes(candidate) > _options.MaxBytes) { await FlushPendingAsync(pending); pending.Add(candidate); deadline = DateTime.UtcNow + _options.MaxDelay.Value; }
                    else pending.Add(candidate);
                    if (pending.Count >= _options.MaxMessages) break;
                }
                if (pending.Count >= _options.MaxMessages) break;
                var remaining = deadline - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero) break;
                var available = _queue.Reader.WaitToReadAsync().AsTask();
                if (await Task.WhenAny(available, Task.Delay(remaining)) != available) break;
                if (!await available) break;
            }
            await FlushPendingAsync(pending);
        flushed:;
        }
        await FlushPendingAsync(pending);
    }

    private async Task<Exception?> FlushPendingAsync(List<Item> pending)
    {
        if (pending.Count == 0) return null;
        var batch = pending.ToArray();
        pending.Clear();
        foreach (var cancelled in batch.Where(item => item.Completion.Task.IsCanceled)) cancelled.Cancellation.Dispose();
        batch = batch.Where(item => !item.Completion.Task.IsCanceled).ToArray();
        if (batch.Length == 0) return null;
        try
        {
            var options = batch[0].Options with { Key = null };
            var result = await _client.PublishBatchAsync(batch[0].Topic, batch.Select(x => new BatchEntry(x.Payload, x.Options.Key)).ToArray(), options, _shutdown.Token);
            if (result.Ids.Length != batch.Length) throw new InvalidDataException("Spruce returned an invalid batch result");
            for (var i = 0; i < batch.Length; i++) { batch[i].Completion.TrySetResult(new PublishResult(result.Ids[i], false)); batch[i].Cancellation.Dispose(); }
        }
        catch (Exception error) { foreach (var item in batch) { item.Completion.TrySetException(error); item.Cancellation.Dispose(); } return error; }
        return null;
    }

    private static bool Compatible(Item left, Item right) => left.Topic == right.Topic && (left.Options with { Key = null }) == (right.Options with { Key = null });
    private static int Bytes(List<Item> items) => items.Sum(x => x.Payload.Length + 6 + System.Text.Encoding.UTF8.GetByteCount(x.Options.Key ?? ""));
    private static int EntryBytes(Item item) => item.Payload.Length + 6 + System.Text.Encoding.UTF8.GetByteCount(item.Options.Key ?? "");
}
