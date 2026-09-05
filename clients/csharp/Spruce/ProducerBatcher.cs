using System.Threading.Channels;

namespace Spruce;

public sealed record ProducerBatcherOptions(int MaxMessages = 256, int MaxBytes = 1 << 20, TimeSpan? MaxDelay = null, int QueueDepth = 4096);

public sealed class ProducerBatcher : IAsyncDisposable
{
    private abstract record Command;
    private sealed record Item(string Topic, byte[] Payload, PublishOptions Options, TaskCompletionSource<PublishResult> Completion, CancellationTokenRegistration Cancellation, int Bytes) : Command;
    private sealed record Barrier(TaskCompletionSource Completion) : Command;

    private readonly SpruceClient _client;
    private readonly ProducerBatcherOptions _options;
    private readonly Channel<Command> _queue;
    private readonly Task _worker;
    private readonly CancellationTokenSource _shutdown = new();
    private int _closed;
    private int _pendingBytes;
    private readonly SemaphoreSlim _queueSlots;
    private readonly CancellationTokenSource _closing = new();

    public ProducerBatcher(SpruceClient client, ProducerBatcherOptions? options = null)
    {
        _client = client;
        options ??= new();
        if (options.MaxMessages is < 1 or > 4096) throw new ArgumentOutOfRangeException(nameof(options));
        if (options.MaxBytes is < 5 or > 16 << 20) throw new ArgumentOutOfRangeException(nameof(options));
        if (options.QueueDepth is < 1 or > 65536) throw new ArgumentOutOfRangeException(nameof(options));
        _options = options with { MaxDelay = options.MaxDelay ?? TimeSpan.FromMicroseconds(250) };
        if (_options.MaxDelay <= TimeSpan.Zero || _options.MaxDelay > TimeSpan.FromDays(1)) throw new ArgumentOutOfRangeException(nameof(options));
        _queueSlots = new SemaphoreSlim(options.QueueDepth, options.QueueDepth);
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
        using var admission = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _closing.Token);
        await _queueSlots.WaitAsync(admission.Token);
        var completion = new TaskCompletionSource<PublishResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var registration = cancellationToken.Register(() => completion.TrySetCanceled(cancellationToken));
        try { await _queue.Writer.WriteAsync(new Item(topic, payload.ToArray(), options, completion, registration, payload.Length + keyBytes + 6), cancellationToken); }
        catch { registration.Dispose(); _queueSlots.Release(); throw; }
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
        _closing.Cancel();
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
            _queueSlots.Release();
            if (pending.Count > 0 && (!Compatible(pending[0], item) || pending.Count >= _options.MaxMessages || _pendingBytes + EntryBytes(item) > _options.MaxBytes)) await FlushPendingAsync(pending);
            pending.Add(item); _pendingBytes += EntryBytes(item);
            var started = System.Diagnostics.Stopwatch.GetTimestamp();
            while (pending.Count < _options.MaxMessages && _pendingBytes < _options.MaxBytes)
            {
                while (_queue.Reader.TryRead(out var next))
                {
                    if (next is Barrier nextBarrier) { var error = await FlushPendingAsync(pending); if (error is null) nextBarrier.Completion.TrySetResult(); else nextBarrier.Completion.TrySetException(error); goto flushed; }
                    var candidate = (Item)next;
                    _queueSlots.Release();
                    if (!Compatible(pending[0], candidate) || _pendingBytes + EntryBytes(candidate) > _options.MaxBytes) { await FlushPendingAsync(pending); pending.Add(candidate); _pendingBytes += EntryBytes(candidate); started = System.Diagnostics.Stopwatch.GetTimestamp(); }
                    else { pending.Add(candidate); _pendingBytes += EntryBytes(candidate); }
                    if (pending.Count >= _options.MaxMessages) break;
                }
                if (pending.Count >= _options.MaxMessages) break;
                var remaining = _options.MaxDelay!.Value - System.Diagnostics.Stopwatch.GetElapsedTime(started);
                if (remaining <= TimeSpan.Zero) break;
                using (var wait = new CancellationTokenSource(remaining))
                {
                    try { if (!await _queue.Reader.WaitToReadAsync(wait.Token)) break; }
                    catch (OperationCanceledException) when (wait.IsCancellationRequested) { break; }
                }
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
        _pendingBytes = 0;
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
    private static int EntryBytes(Item item) => item.Bytes;
}
