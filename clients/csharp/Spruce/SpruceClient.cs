using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.IO.Compression;
using System.Collections.Concurrent;
using ZstdSharp;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;

namespace Spruce;

public sealed record PublishOptions(string? Key = null, TimeSpan? Ttl = null, string? ContentType = null, string? Ack = null, string? IdempotencyKey = null, string? ProducerId = null, string? Compression = null);
public sealed record PublishResult([property: JsonPropertyName("id")] string Id, [property: JsonPropertyName("replicated")] bool Replicated, [property: JsonPropertyName("confirmed_copies")] int ConfirmedCopies = 0, [property: JsonPropertyName("degraded")] bool Degraded = false);
public sealed record BatchResult([property: JsonPropertyName("ids")] string[] Ids);
public sealed record BatchEntry(ReadOnlyMemory<byte> Payload, string? Key = null);
public sealed record Delivery(
    [property: JsonPropertyName("delivery_id")] string DeliveryId,
    [property: JsonPropertyName("message_id")] string MessageId,
    [property: JsonPropertyName("topic")] string Topic,
    [property: JsonPropertyName("key")] string? Key,
    [property: JsonPropertyName("headers")] Dictionary<string, string>? Headers,
    [property: JsonPropertyName("created_at")] long CreatedAt,
    [property: JsonPropertyName("attempt")] int Attempt,
    [property: JsonPropertyName("cursor")] string? Cursor = null)
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
        request.Content = new ByteArrayContent(EncodePayload(payload.Span, options.Compression));
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
        => await PublishBatchAsync(topic, payloads.Select(payload => new BatchEntry(payload, options?.Key)).ToArray(), options, cancellationToken);

    public async Task<BatchResult> PublishBatchAsync(string topic, IReadOnlyList<BatchEntry> entries, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_requestTimeout);
        if (entries.Count == 0) throw new ArgumentException("Batch is empty", nameof(entries));
        if (entries.Count > 4096) throw new ArgumentException("Batch exceeds 4096 messages", nameof(entries));
        options ??= new();
        if (options.Ack is not null and not "local") throw new ArgumentException("Batch only supports local acknowledgement", nameof(options));
        if (options.IdempotencyKey is not null) throw new ArgumentException("Batch idempotency is not supported", nameof(options));
        var encodedEntries = entries.Select(entry => new BatchEntry(EncodePayload(entry.Payload.Span, options.Compression), entry.Key)).ToArray();
        var total = encodedEntries.Aggregate(0L, (sum, entry) => checked(sum + 6 + entry.Payload.Length + System.Text.Encoding.UTF8.GetByteCount(entry.Key ?? "")));
        if (entries.Any(entry => entry.Payload.Length > 1024 * 1024 || System.Text.Encoding.UTF8.GetByteCount(entry.Key ?? "") > 8 * 1024)) throw new ArgumentException("A batch entry exceeds its limit", nameof(entries));
        if (total > 16 * 1024 * 1024) throw new ArgumentException("Batch exceeds 16 MiB", nameof(entries));
        using var body = new MemoryStream(checked((int)total));
        var size = new byte[4];
        foreach (var entry in encodedEntries)
        {
            var key = System.Text.Encoding.UTF8.GetBytes(entry.Key ?? "");
            var keySize = new byte[2]; BinaryPrimitives.WriteUInt16BigEndian(keySize, checked((ushort)key.Length));
            await body.WriteAsync(keySize, timeout.Token);
            await body.WriteAsync(key, timeout.Token);
            BinaryPrimitives.WriteUInt32BigEndian(size, checked((uint)entry.Payload.Length));
            await body.WriteAsync(size, timeout.Token);
            await body.WriteAsync(entry.Payload, timeout.Token);
        }
        body.Position = 0;
        using var request = new HttpRequestMessage(HttpMethod.Post, $"{_baseUrl}/v1/topics/{Uri.EscapeDataString(topic)}/batches") { Content = new StreamContent(body) };
        request.Headers.Add("Spruce-Batch-Version", "2");
        Authorize(request);
        if (options.ContentType is not null) request.Content.Headers.ContentType = new(options.ContentType);
        if (options.Ttl is not null) request.Headers.Add("Spruce-TTL", $"{options.Ttl.Value.TotalMilliseconds}ms");
        using var response = await SendAsync(request, HttpCompletionOption.ResponseContentRead, timeout.Token, "publish_batch");
        await EnsureSuccessAsync(response, timeout.Token);
        return (await response.Content.ReadFromJsonAsync<BatchResult>(timeout.Token))!;
    }

    public Task SubscribeAsync(string topic, string? group, Func<Delivery, CancellationToken, Task> handler, CancellationToken cancellationToken = default, int concurrency = 16, int maxPayloadBytes = 1024 * 1024, long since = 0, TimeSpan? drainTimeout = null, bool preserveKeyOrder = false, string? cursor = null)
        => SubscribeCoreAsync(topic, group, Guid.NewGuid().ToString("N"), handler, cancellationToken, concurrency, maxPayloadBytes, since, drainTimeout, preserveKeyOrder, cursor);

    public Task SubscribeAsync(string topic, string? group, string memberId, Func<Delivery, CancellationToken, Task> handler, CancellationToken cancellationToken = default, int concurrency = 16, int maxPayloadBytes = 1024 * 1024, long since = 0, TimeSpan? drainTimeout = null, bool preserveKeyOrder = false, string? cursor = null)
        => SubscribeCoreAsync(topic, group, memberId, handler, cancellationToken, concurrency, maxPayloadBytes, since, drainTimeout, preserveKeyOrder, cursor);

    private async Task SubscribeCoreAsync(string topic, string? group, string memberId, Func<Delivery, CancellationToken, Task> handler, CancellationToken cancellationToken, int concurrency, int maxPayloadBytes, long since, TimeSpan? drainTimeout, bool preserveKeyOrder, string? cursor)
    {
        if (string.IsNullOrWhiteSpace(topic)) throw new ArgumentException("Topic is required", nameof(topic));
        if (concurrency <= 0) concurrency = 16;
        if (concurrency < 1 || concurrency > 1024) throw new ArgumentOutOfRangeException(nameof(concurrency));
        if (maxPayloadBytes < 1 || maxPayloadBytes > 64 * 1024 * 1024) throw new ArgumentOutOfRangeException(nameof(maxPayloadBytes));
        drainTimeout ??= TimeSpan.FromSeconds(1);
        if (drainTimeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(drainTimeout));
		if (since < 0) throw new ArgumentOutOfRangeException(nameof(since));
		if (since != 0 && cursor is not null) throw new ArgumentException("Specify either since or cursor, not both");
		var legacyTimestampCursor = since != 0;
		if (string.IsNullOrEmpty(memberId) || System.Text.Encoding.UTF8.GetByteCount(memberId) > 255) throw new ArgumentException("Member ID must contain 1 to 255 UTF-8 bytes", nameof(memberId));
        var backoff = TimeSpan.FromMilliseconds(50);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
				var connectedAt = DateTime.UtcNow;
                var uri = $"{_baseUrl}/v1/subscriptions/stream?topic={Uri.EscapeDataString(topic)}" +
                    (group is null ? "" : $"&group={Uri.EscapeDataString(group)}") +
                    $"&member={Uri.EscapeDataString(memberId)}" +
                    (legacyTimestampCursor ? $"&since={since}" : cursor is null ? "" : $"&cursor={Uri.EscapeDataString(cursor)}");
                using var request = new HttpRequestMessage(HttpMethod.Get, uri);
                request.Headers.Add("Spruce-Delivery-Affinity", CompletionAffinity(topic, group));
                Authorize(request);
                using var response = await SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken, "subscribe");
                await EnsureSuccessAsync(response, cancellationToken);
				if (!legacyTimestampCursor) cursor ??= response.Headers.TryGetValues("Spruce-Cursor", out var initialCursors) ? initialCursors.FirstOrDefault() : null;
                Emit(new("subscription_connected", TimeSpan.Zero, (int)response.StatusCode, null));
                await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
                using var connection = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                await using var acks = new AckBatcher(this, "ack", connection.Token, topic, group);
                await using var nacks = new AckBatcher(this, "nack", connection.Token, topic, group);
                var deliveryLanes = preserveKeyOrder
                    ? Enumerable.Range(0, concurrency).Select(_ => Channel.CreateBounded<(Delivery Delivery, long Index)>(new BoundedChannelOptions(2) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true })).ToArray()
                    : [Channel.CreateBounded<(Delivery Delivery, long Index)>(new BoundedChannelOptions(concurrency * 2) { FullMode = BoundedChannelFullMode.Wait, SingleWriter = true })];
                var progressLock = new object();
                var completed = new SortedDictionary<long, (string? Cursor, long CreatedAt)>();
                using var progressWindow = new SemaphoreSlim(concurrency * 2, concurrency * 2);
                long sequence = 0, nextProgress = 1;
                var connectedCursor = cursor;
				var connectedSince = since;
                async Task ConsumeAsync(ChannelReader<(Delivery Delivery, long Index)> reader)
                {
                    try
                    {
                        await foreach (var work in reader.ReadAllAsync(connection.Token))
                        {
                            try { await handler(work.Delivery, connection.Token); }
                            catch when (!connection.IsCancellationRequested)
                            {
                                await nacks.SubmitAsync(work.Delivery.DeliveryId, connection.Token);
                                // A rejected handler has not completed this position.
                                // Reconnect before it rather than skip a lost broker retry.
                                connection.Cancel();
                                return;
                            }
                            await acks.SubmitAsync(work.Delivery.DeliveryId, connection.Token);
                            MarkComplete(work.Index, work.Delivery.Cursor, work.Delivery.CreatedAt);
                        }
                    }
                    catch when (!connection.IsCancellationRequested)
                    {
                        connection.Cancel();
                        throw;
                    }
                }
                void MarkComplete(long index, string? completedCursor, long createdAt)
                {
					var advanced = 0;
                    lock (progressLock)
                    {
                        completed[index] = (completedCursor, createdAt);
                        while (completed.Remove(nextProgress, out var value))
                        {
							if (legacyTimestampCursor) since = Math.Max(since, value.CreatedAt);
							else if (!string.IsNullOrEmpty(value.Cursor)) cursor = value.Cursor;
                            nextProgress++;
							advanced++;
                        }
                    }
					if (advanced > 0) progressWindow.Release(advanced);
                }
                var workers = preserveKeyOrder
                    ? deliveryLanes.Select(lane => ConsumeAsync(lane.Reader)).ToArray()
                    : Enumerable.Range(0, concurrency).Select(_ => ConsumeAsync(deliveryLanes[0].Reader)).ToArray();
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
                        delivery = delivery with { Payload = DecodePayload(payload, maxPayloadBytes) };
						await progressWindow.WaitAsync(connection.Token);
                        var index = Interlocked.Increment(ref sequence);
                        var lane = preserveKeyOrder ? StableLane(delivery.Key ?? delivery.MessageId, deliveryLanes.Length) : 0;
                        await deliveryLanes[lane].Writer.WriteAsync((delivery, index), connection.Token);
                    }
                }
                catch (EndOfStreamException)
                {
                    gracefulEnd = true;
                }
                finally
                {
                    foreach (var lane in deliveryLanes) lane.Writer.TryComplete();
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
                    if ((legacyTimestampCursor ? since > connectedSince : cursor != connectedCursor) || DateTime.UtcNow - connectedAt >= TimeSpan.FromSeconds(5)) backoff = TimeSpan.FromMilliseconds(50);
                }
                if (gracefulEnd && !cancellationToken.IsCancellationRequested)
                {
                    var jitter = 0.5 + Random.Shared.NextDouble();
                    await Task.Delay(TimeSpan.FromMilliseconds(backoff.TotalMilliseconds * jitter), cancellationToken);
                    backoff = TimeSpan.FromMilliseconds(Math.Min(2000, backoff.TotalMilliseconds * 2));
                }
            }
            catch (SpruceException ex) when (!cancellationToken.IsCancellationRequested && ex.Code == "cursor_expired")
            {
                Emit(new("subscription_cursor_expired", TimeSpan.Zero, ex.StatusCode, ex));
                Emit(new("subscription_disconnected", TimeSpan.Zero, ex.StatusCode, ex));
                throw;
            }
            catch (HttpRequestException ex) when (!cancellationToken.IsCancellationRequested && ex.StatusCode is null or >= System.Net.HttpStatusCode.InternalServerError)
            {
                Emit(new("subscription_disconnected", TimeSpan.Zero, ex.StatusCode is null ? null : (int)ex.StatusCode, ex));
                Emit(new("subscription_reconnecting", TimeSpan.Zero, null, null));
                var jitter = 0.5 + Random.Shared.NextDouble();
                await Task.Delay(TimeSpan.FromMilliseconds(backoff.TotalMilliseconds * jitter), cancellationToken);
                backoff = TimeSpan.FromMilliseconds(Math.Min(2000, backoff.TotalMilliseconds * 2));
            }
            catch (SpruceException ex) when (!cancellationToken.IsCancellationRequested && (ex.StatusCode == 408 || ex.StatusCode == 429 || ex.StatusCode >= 500))
            {
                Emit(new("subscription_disconnected", TimeSpan.Zero, ex.StatusCode, ex));
                Emit(new("subscription_reconnecting", TimeSpan.Zero, null, null));
                var jitter = 0.5 + Random.Shared.NextDouble();
                await Task.Delay(TimeSpan.FromMilliseconds(Math.Max(ex.RetryAfter.TotalMilliseconds, Math.Min(2000, backoff.TotalMilliseconds * jitter))), cancellationToken);
                backoff = TimeSpan.FromMilliseconds(Math.Min(2000, backoff.TotalMilliseconds * 2));
            }
            catch (IOException) when (!cancellationToken.IsCancellationRequested)
            {
                Emit(new("subscription_disconnected", TimeSpan.Zero, null, null));
                Emit(new("subscription_reconnecting", TimeSpan.Zero, null, null));
                var jitter = 0.5 + Random.Shared.NextDouble();
                await Task.Delay(TimeSpan.FromMilliseconds(backoff.TotalMilliseconds * jitter), cancellationToken);
                backoff = TimeSpan.FromMilliseconds(Math.Min(2000, backoff.TotalMilliseconds * 2));
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
				Emit(new("subscription_disconnected", TimeSpan.Zero, null, new OperationCanceledException("Spruce subscription connection cancelled unexpectedly")));
				Emit(new("subscription_reconnecting", TimeSpan.Zero, null, null));
                var jitter = 0.5 + Random.Shared.NextDouble();
                await Task.Delay(TimeSpan.FromMilliseconds(backoff.TotalMilliseconds * jitter), cancellationToken);
                backoff = TimeSpan.FromMilliseconds(Math.Min(2000, backoff.TotalMilliseconds * 2));
            }
        }
    }

    private static int StableLane(string key, int laneCount)
    {
        uint hash = 2166136261;
        foreach (var value in System.Text.Encoding.UTF8.GetBytes(key)) hash = (hash ^ value) * 16777619;
        return (int)(hash % (uint)laneCount);
    }

    private static string CompletionAffinity(string topic, string? group) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(topic + "\0" + (group ?? "")))).ToLowerInvariant();

    private async Task AckAsync(string action, IReadOnlyList<string> ids, CancellationToken cancellationToken, string topic, string? group)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        using var request = new HttpRequestMessage(HttpMethod.Post, $"{_baseUrl}/v1/deliveries/{action}") { Content = JsonContent.Create(new { delivery_ids = ids }) };
        request.Headers.Add("Spruce-Delivery-Affinity", CompletionAffinity(topic, group));
        Authorize(request);
        using var response = await SendAsync(request, HttpCompletionOption.ResponseContentRead, timeout.Token, action);
        await EnsureSuccessAsync(response, timeout.Token);
    }

    private void Authorize(HttpRequestMessage request)
    {
        if (_token is not null) request.Headers.Authorization = new("Bearer", _token);
        else if (_username is not null) request.Headers.Authorization = new("Basic", Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes($"{_username}:{_password}")));
    }

    private static readonly byte[] CompressionMagic = [0x89, (byte)'S', (byte)'P', (byte)'R', (byte)'U', (byte)'C', (byte)'E', 0x01];
    private static readonly ConcurrentBag<Compressor> ZstdCompressors = [];
    private static readonly ConcurrentBag<Decompressor> ZstdDecompressors = [];

    private static byte[] EncodePayload(ReadOnlySpan<byte> payload, string? algorithm)
    {
        if (payload.Length < 1024 || algorithm == "off") return payload.ToArray();
        algorithm ??= "zstd";
        if (algorithm is not ("gzip" or "zstd")) throw new ArgumentException($"Unsupported compression '{algorithm}'", nameof(algorithm));
        using var output = new MemoryStream();
        output.Write(CompressionMagic);
        output.WriteByte(algorithm == "gzip" ? (byte)1 : (byte)2);
        Span<byte> size = stackalloc byte[4]; BinaryPrimitives.WriteUInt32BigEndian(size, checked((uint)payload.Length)); output.Write(size);
        if (algorithm == "gzip")
        {
            using var gzip = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true);
            gzip.Write(payload);
        }
        else
        {
            if (!ZstdCompressors.TryTake(out var compressor)) compressor = new Compressor(1);
            try { output.Write(compressor.Wrap(payload)); }
            finally { ZstdCompressors.Add(compressor); }
        }
        var encoded = output.ToArray();
        var minimumSaving = Math.Max(128, payload.Length / 10);
        return encoded.Length <= payload.Length - minimumSaving ? encoded : payload.ToArray();
    }

    private static byte[] DecodePayload(byte[] payload, int maximum)
    {
        if (payload.Length < CompressionMagic.Length + 5 || !payload.AsSpan(0, CompressionMagic.Length).SequenceEqual(CompressionMagic)) return payload;
        var algorithm = payload[CompressionMagic.Length];
        if (algorithm is not (1 or 2)) throw new InvalidDataException("Unsupported compressed payload encoding");
        var original = checked((int)BinaryPrimitives.ReadUInt32BigEndian(payload.AsSpan(CompressionMagic.Length + 1, 4)));
        if (original > maximum) throw new InvalidDataException("Compressed payload exceeds decompressed limit");
        if (algorithm == 2)
        {
            try
            {
                if (!ZstdDecompressors.TryTake(out var decompressor)) decompressor = new Decompressor();
                var zstdDecoded = new byte[original];
                int written;
                try { written = decompressor.Unwrap(payload.AsSpan(CompressionMagic.Length + 5), zstdDecoded); }
                finally { ZstdDecompressors.Add(decompressor); }
                if (written != original) throw new InvalidDataException("Invalid decompressed payload length");
                return zstdDecoded;
            }
            catch (InvalidDataException) { throw; }
            catch (Exception exception) { throw new InvalidDataException("Invalid Zstandard payload", exception); }
        }
        using var input = new MemoryStream(payload, CompressionMagic.Length + 5, payload.Length - CompressionMagic.Length - 5, false);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream(Math.Min(original, maximum));
        var buffer = new byte[8192];
        while (output.Length <= maximum)
        {
            var read = gzip.Read(buffer, 0, Math.Min(buffer.Length, maximum + 1 - checked((int)output.Length)));
            if (read == 0) break;
            output.Write(buffer, 0, read);
        }
        var decoded = output.ToArray();
        if (decoded.Length != original || decoded.Length > maximum) throw new InvalidDataException("Invalid decompressed payload length");
        return decoded;
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
        TimeSpan retryAfter = response.Headers.RetryAfter?.Delta ?? (response.Headers.RetryAfter?.Date is DateTimeOffset deadline ? deadline - DateTimeOffset.UtcNow : TimeSpan.Zero);
        if (retryAfter < TimeSpan.Zero) retryAfter = TimeSpan.Zero;
        throw new SpruceException((int)response.StatusCode, response.ReasonPhrase ?? response.StatusCode.ToString(), code, body, retryAfter);
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
        private readonly string _topic;
        private readonly string? _group;
        private readonly Channel<Item> _items = Channel.CreateBounded<Item>(new BoundedChannelOptions(1024) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });
        private readonly CancellationToken _cancellationToken;
        private readonly Task _worker;

        public AckBatcher(SpruceClient client, string action, CancellationToken cancellationToken, string topic, string? group)
        {
            _client = client;
            _action = action;
            _topic = topic; _group = group;
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
                    try { await _client.AckAsync(_action, batch.Select(item => item.Id).ToArray(), _cancellationToken, _topic, _group); }
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
