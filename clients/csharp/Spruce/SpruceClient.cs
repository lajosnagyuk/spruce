using System.Buffers.Binary;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;

namespace Spruce;

public sealed record PublishOptions(string? Key = null, TimeSpan? Ttl = null, string? ContentType = null, string? Ack = null, string? IdempotencyKey = null, string? ProducerId = null);
public sealed record PublishResult([property: JsonPropertyName("id")] string Id, [property: JsonPropertyName("replicated")] bool Replicated);
public sealed record BatchResult([property: JsonPropertyName("ids")] string[] Ids);
public sealed record Delivery(
    [property: JsonPropertyName("delivery_id")] string DeliveryId,
    [property: JsonPropertyName("message_id")] string MessageId,
    [property: JsonPropertyName("topic")] string Topic,
    [property: JsonPropertyName("key")] string? Key,
    [property: JsonPropertyName("headers")] Dictionary<string, string>? Headers,
    [property: JsonPropertyName("created_at")] long CreatedAt,
    [property: JsonPropertyName("attempt")] int Attempt)
{ public byte[] Payload { get; init; } = []; }

public sealed partial class SpruceClient : IDisposable
{
    private readonly HttpClient _http;
    private readonly string _baseUrl;
    private readonly string? _token;
    private readonly string? _username;
    private readonly string? _password;
    private readonly bool _ownsHttp;
    private readonly TimeSpan _requestTimeout;
    public Action<ClientEvent>? OnEvent { get; init; }

    public SpruceClient(string baseUrl, HttpClient? http = null, string? token = null, string? username = null, string? password = null, bool allowInsecureCredentials = false, TimeSpan? requestTimeout = null)
    {
        _baseUrl = baseUrl.TrimEnd('/');
        if (!allowInsecureCredentials && (token is not null || username is not null))
        {
            if (!Uri.TryCreate(_baseUrl, UriKind.Absolute, out var endpoint) || endpoint.Scheme != Uri.UriSchemeHttps)
                throw new ArgumentException("Credentials require an HTTPS base URL; use allowInsecureCredentials only for isolated development", nameof(baseUrl));
        }
        _ownsHttp = http is null;
        _http = http ?? new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
        _token = token;
        _username = username;
        _password = password;
        _requestTimeout = requestTimeout ?? TimeSpan.FromSeconds(30);
        if (_requestTimeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(requestTimeout));
        if ((username is null) != (password is null)) throw new ArgumentException("Basic username and password must be supplied together");
    }

    public void Dispose() { if (_ownsHttp) _http.Dispose(); }

    public async Task<PublishResult> PublishAsync(string topic, ReadOnlyMemory<byte> payload, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_requestTimeout);
        options ??= new();
        using var request = new HttpRequestMessage(HttpMethod.Post, $"{_baseUrl}/v1/topics/{Uri.EscapeDataString(topic)}/messages");
        request.Content = new ByteArrayContent(payload.ToArray());
        if (options.ContentType is not null) request.Content.Headers.ContentType = new(options.ContentType);
        if (options.Key is not null) request.Headers.Add("Spruce-Key", options.Key);
        if (options.Ttl is not null) request.Headers.Add("Spruce-TTL", $"{options.Ttl.Value.TotalMilliseconds}ms");
        if (options.Ack is not null) request.Headers.Add("Spruce-Ack", options.Ack);
        if (options.IdempotencyKey is not null) request.Headers.Add("Spruce-Idempotency-Key", options.IdempotencyKey);
        if (options.ProducerId is not null) request.Headers.Add("Spruce-Producer-ID", options.ProducerId);
        Authorize(request);
        using var response = await SendAsync(request, HttpCompletionOption.ResponseContentRead, timeout.Token, "publish");
        await EnsureSuccessAsync(response, timeout.Token);
        return (await response.Content.ReadFromJsonAsync<PublishResult>(timeout.Token))!;
    }

    public async Task<BatchResult> PublishBatchAsync(string topic, IReadOnlyList<ReadOnlyMemory<byte>> payloads, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_requestTimeout);
        if (payloads.Count == 0) throw new ArgumentException("Batch is empty", nameof(payloads));
        if (payloads.Count > 4096) throw new ArgumentException("Batch exceeds 4096 messages", nameof(payloads));
        options ??= new();
        if (options.Ack is not null and not "local") throw new ArgumentException("Batch only supports local acknowledgement", nameof(options));
        if (options.IdempotencyKey is not null) throw new ArgumentException("Batch idempotency is not supported", nameof(options));
        var total = payloads.Aggregate(0L, (sum, payload) => checked(sum + 4 + payload.Length));
        if (payloads.Any(payload => payload.Length > 1024 * 1024)) throw new ArgumentException("A batch message exceeds 1 MiB", nameof(payloads));
        if (total > 16 * 1024 * 1024) throw new ArgumentException("Batch exceeds 16 MiB", nameof(payloads));
        using var body = new MemoryStream(checked((int)total));
        var size = new byte[4];
        foreach (var payload in payloads)
        {
            BinaryPrimitives.WriteUInt32BigEndian(size, checked((uint)payload.Length));
            await body.WriteAsync(size, timeout.Token);
            await body.WriteAsync(payload, timeout.Token);
        }
        body.Position = 0;
        using var request = new HttpRequestMessage(HttpMethod.Post, $"{_baseUrl}/v1/topics/{Uri.EscapeDataString(topic)}/batches") { Content = new StreamContent(body) };
        Authorize(request);
        if (options.ContentType is not null) request.Content.Headers.ContentType = new(options.ContentType);
        if (options.Key is not null) request.Headers.Add("Spruce-Key", options.Key);
        if (options.Ttl is not null) request.Headers.Add("Spruce-TTL", $"{options.Ttl.Value.TotalMilliseconds}ms");
        using var response = await SendAsync(request, HttpCompletionOption.ResponseContentRead, timeout.Token, "publish_batch");
        await EnsureSuccessAsync(response, timeout.Token);
        return (await response.Content.ReadFromJsonAsync<BatchResult>(timeout.Token))!;
    }

    public async Task SubscribeAsync(string topic, string? group, Func<Delivery, CancellationToken, Task> handler, CancellationToken cancellationToken = default, int concurrency = 16, int maxPayloadBytes = 1024 * 1024, long since = 0, TimeSpan? drainTimeout = null)
    {
        if (string.IsNullOrWhiteSpace(topic)) throw new ArgumentException("Topic is required", nameof(topic));
        if (concurrency <= 0) concurrency = 16;
        if (concurrency < 1 || concurrency > 1024) throw new ArgumentOutOfRangeException(nameof(concurrency));
        if (maxPayloadBytes < 1 || maxPayloadBytes > 64 * 1024 * 1024) throw new ArgumentOutOfRangeException(nameof(maxPayloadBytes));
        drainTimeout ??= TimeSpan.FromSeconds(1);
        if (drainTimeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(drainTimeout));
        var backoff = TimeSpan.FromMilliseconds(50);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var uri = $"{_baseUrl}/v1/subscriptions/stream?topic={Uri.EscapeDataString(topic)}" +
                    (group is null ? "" : $"&group={Uri.EscapeDataString(group)}") +
                    (since == 0 ? "" : $"&since={since}");
                using var request = new HttpRequestMessage(HttpMethod.Get, uri);
                Authorize(request);
                using var response = await SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken, "subscribe");
                await EnsureSuccessAsync(response, cancellationToken);
                await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
                using var connection = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                await using var acks = new AckBatcher(this, "ack", connection.Token);
                await using var nacks = new AckBatcher(this, "nack", connection.Token);
                var deliveries = Channel.CreateBounded<(Delivery Delivery, long Index)>(new BoundedChannelOptions(concurrency * 2) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true });
                var progressLock = new object();
                var completed = new SortedDictionary<long, long>();
                long sequence = 0, nextProgress = 1;
                var connectedSince = since;
                async Task ConsumeAsync()
                {
                    try
                    {
                        await foreach (var work in deliveries.Reader.ReadAllAsync(connection.Token))
                        {
                            try { await handler(work.Delivery, connection.Token); }
                            catch when (!connection.IsCancellationRequested)
                            {
                                await nacks.SubmitAsync(work.Delivery.DeliveryId, connection.Token);
                                MarkComplete(work.Index, work.Delivery.CreatedAt);
                                continue;
                            }
                            await acks.SubmitAsync(work.Delivery.DeliveryId, connection.Token);
                            MarkComplete(work.Index, work.Delivery.CreatedAt);
                        }
                    }
                    catch when (!connection.IsCancellationRequested)
                    {
                        connection.Cancel();
                        throw;
                    }
                }
                void MarkComplete(long index, long createdAt)
                {
                    lock (progressLock)
                    {
                        completed[index] = createdAt;
                        while (completed.Remove(nextProgress, out var created))
                        {
                            since = Math.Max(since, created);
                            nextProgress++;
                        }
                    }
                }
                var workers = Enumerable.Range(0, concurrency).Select(_ => ConsumeAsync()).ToArray();
                var sizes = new byte[8];
                var gracefulEnd = false;
                try
                {
                    while (!connection.IsCancellationRequested)
                    {
                        await stream.ReadExactlyAsync(sizes, connection.Token);
                        var metadataLength = BinaryPrimitives.ReadUInt32BigEndian(sizes.AsSpan(0, 4));
                        var payloadLength = BinaryPrimitives.ReadUInt32BigEndian(sizes.AsSpan(4, 4));
                        if (metadataLength > 65536 || payloadLength > maxPayloadBytes) throw new InvalidDataException("Invalid Spruce frame size");
                        var metadata = new byte[metadataLength]; await stream.ReadExactlyAsync(metadata, connection.Token);
                        var delivery = JsonSerializer.Deserialize<Delivery>(metadata)!;
                        var payload = new byte[payloadLength]; await stream.ReadExactlyAsync(payload, connection.Token);
                        if (string.IsNullOrEmpty(delivery.DeliveryId)) continue;
                        delivery = delivery with { Payload = payload };
                        var index = Interlocked.Increment(ref sequence);
                        await deliveries.Writer.WriteAsync((delivery, index), connection.Token);
                    }
                }
                catch (EndOfStreamException)
                {
                    gracefulEnd = true;
                }
                finally
                {
                    deliveries.Writer.TryComplete();
                    if (!gracefulEnd) connection.Cancel();
                    var joined = Task.WhenAll(workers);
                    if (!joined.IsCompleted)
                    {
                        if (await Task.WhenAny(joined, Task.Delay(drainTimeout.Value)) != joined)
                        {
                            connection.Cancel();
                            throw new TimeoutException("Spruce handlers did not stop before the drain timeout");
                        }
                    }
                    try { await joined; }
                    catch (OperationCanceledException) when (connection.IsCancellationRequested) { }
                    if (since > connectedSince) backoff = TimeSpan.FromMilliseconds(50);
                }
                if (gracefulEnd && !cancellationToken.IsCancellationRequested)
                {
                    var jitter = 0.5 + Random.Shared.NextDouble();
                    await Task.Delay(TimeSpan.FromMilliseconds(backoff.TotalMilliseconds * jitter), cancellationToken);
                    backoff = TimeSpan.FromMilliseconds(Math.Min(2000, backoff.TotalMilliseconds * 2));
                }
            }
            catch (HttpRequestException ex) when (!cancellationToken.IsCancellationRequested && ex.StatusCode is null or >= System.Net.HttpStatusCode.InternalServerError)
            {
                var jitter = 0.5 + Random.Shared.NextDouble();
                await Task.Delay(TimeSpan.FromMilliseconds(backoff.TotalMilliseconds * jitter), cancellationToken);
                backoff = TimeSpan.FromMilliseconds(Math.Min(2000, backoff.TotalMilliseconds * 2));
            }
            catch (SpruceException ex) when (!cancellationToken.IsCancellationRequested && (ex.StatusCode == 408 || ex.StatusCode == 429 || ex.StatusCode >= 500))
            {
                var jitter = 0.5 + Random.Shared.NextDouble();
                await Task.Delay(TimeSpan.FromMilliseconds(backoff.TotalMilliseconds * jitter), cancellationToken);
                backoff = TimeSpan.FromMilliseconds(Math.Min(2000, backoff.TotalMilliseconds * 2));
            }
            catch (IOException) when (!cancellationToken.IsCancellationRequested)
            {
                var jitter = 0.5 + Random.Shared.NextDouble();
                await Task.Delay(TimeSpan.FromMilliseconds(backoff.TotalMilliseconds * jitter), cancellationToken);
                backoff = TimeSpan.FromMilliseconds(Math.Min(2000, backoff.TotalMilliseconds * 2));
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                var jitter = 0.5 + Random.Shared.NextDouble();
                await Task.Delay(TimeSpan.FromMilliseconds(backoff.TotalMilliseconds * jitter), cancellationToken);
                backoff = TimeSpan.FromMilliseconds(Math.Min(2000, backoff.TotalMilliseconds * 2));
            }
        }
    }

    private async Task AckAsync(string action, IReadOnlyList<string> ids, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        using var request = new HttpRequestMessage(HttpMethod.Post, $"{_baseUrl}/v1/deliveries/{action}") { Content = JsonContent.Create(new { delivery_ids = ids }) };
        Authorize(request);
        using var response = await SendAsync(request, HttpCompletionOption.ResponseContentRead, timeout.Token, action);
        await EnsureSuccessAsync(response, timeout.Token);
    }

    private void Authorize(HttpRequestMessage request)
    {
        if (_token is not null) request.Headers.Authorization = new("Bearer", _token);
        else if (_username is not null) request.Headers.Authorization = new("Basic", Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{_username}:{_password}")));
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode) return;
        await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        var buffer = new byte[4097];
        var read = await stream.ReadAsync(buffer, cancellationToken);
        var body = System.Text.Encoding.UTF8.GetString(buffer, 0, Math.Min(read, 4096));
        string? code = null;
        try { code = JsonDocument.Parse(body).RootElement.GetProperty("error").GetString(); } catch (Exception) { }
        throw new SpruceException((int)response.StatusCode, response.ReasonPhrase ?? response.StatusCode.ToString(), code, body);
    }

    private async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, HttpCompletionOption completion, CancellationToken cancellationToken, string operation)
    {
        var started = DateTimeOffset.UtcNow;
        var emitted = false;
        try
        {
            var response = await _http.SendAsync(request, completion, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                try { await EnsureSuccessAsync(response, cancellationToken); }
                catch (Exception error) { Emit(new(operation, DateTimeOffset.UtcNow - started, (int)response.StatusCode, error)); emitted = true; response.Dispose(); throw; }
            }
            Emit(new(operation, DateTimeOffset.UtcNow - started, (int)response.StatusCode, null));
            return response;
        }
        catch (Exception ex)
        {
            if (!emitted) Emit(new(operation, DateTimeOffset.UtcNow - started, null, ex));
            throw;
        }
    }

    private void Emit(ClientEvent value)
    {
        try { OnEvent?.Invoke(value); } catch { }
    }

    private sealed class AckBatcher : IAsyncDisposable
    {
        private sealed record Item(string Id, TaskCompletionSource Completion);
        private readonly SpruceClient _client;
        private readonly string _action;
        private readonly Channel<Item> _items = Channel.CreateBounded<Item>(new BoundedChannelOptions(1024) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });
        private readonly CancellationToken _cancellationToken;
        private readonly Task _worker;

        public AckBatcher(SpruceClient client, string action, CancellationToken cancellationToken)
        {
            _client = client;
            _action = action;
            _cancellationToken = cancellationToken;
            _worker = RunAsync();
        }

        public async Task SubmitAsync(string id, CancellationToken cancellationToken)
        {
            var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await _items.Writer.WriteAsync(new Item(id, completion), cancellationToken);
            await completion.Task.WaitAsync(cancellationToken);
        }

        private async Task RunAsync()
        {
            Exception? terminal = null;
            List<Item>? active = null;
            try
            {
                while (await _items.Reader.WaitToReadAsync(_cancellationToken))
                {
                    if (!_items.Reader.TryRead(out var first)) continue;
                    var batch = new List<Item>(256) { first };
                    active = batch;
                    await Task.Delay(TimeSpan.FromMilliseconds(0.5), _cancellationToken);
                    while (batch.Count < 256 && _items.Reader.TryRead(out var item)) batch.Add(item);
                    Exception? failure = null;
                    try { await _client.AckAsync(_action, batch.Select(item => item.Id).ToArray(), _cancellationToken); }
                    catch (Exception ex) { failure = ex; }
                    foreach (var item in batch)
                    {
                        if (failure is null) item.Completion.SetResult();
                        else item.Completion.SetException(failure);
                    }
                    active = null;
                }
            }
            catch (OperationCanceledException ex) when (_cancellationToken.IsCancellationRequested) { terminal = ex; }
            catch (Exception ex) { terminal = ex; }
            finally
            {
                terminal ??= new OperationCanceledException(_cancellationToken);
                if (active is not null)
                    foreach (var item in active) item.Completion.TrySetException(terminal);
                while (_items.Reader.TryRead(out var item)) item.Completion.TrySetException(terminal);
            }
        }

        public async ValueTask DisposeAsync()
        {
            _items.Writer.TryComplete();
            try { await _worker; }
            catch (OperationCanceledException) when (_cancellationToken.IsCancellationRequested) { }
        }
    }
}
