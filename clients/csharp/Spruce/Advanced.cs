using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using System.Threading.Channels;

namespace Spruce;

public sealed record ClientEvent(string Operation, TimeSpan Duration, int? StatusCode, Exception? Error);
public sealed record RetryOptions(int MaxAttempts = 3, TimeSpan? MinBackoff = null, TimeSpan? MaxBackoff = null);
public sealed record SubscribeOptions(
    string Topic,
    string? Group = null,
    long Since = 0,
    int Concurrency = 16,
    int MaxPayloadBytes = 1024 * 1024,
    TimeSpan? DrainTimeout = null);
public sealed record BrokerStatus(
    [property: JsonPropertyName("messages")] int Messages,
    [property: JsonPropertyName("cache_accounted_bytes")] long CacheAccountedBytes,
    [property: JsonPropertyName("cache_limit_bytes")] long CacheLimitBytes,
    [property: JsonPropertyName("peers")] int Peers,
    [property: JsonPropertyName("consumers")] int Consumers,
    [property: JsonPropertyName("pending_deliveries")] int PendingDeliveries);

public sealed class ConsumableDelivery(Delivery delivery)
{
    private readonly TaskCompletionSource<Exception?> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Delivery Delivery { get; } = delivery;
    public void Complete(Exception? error = null) => _completion.TrySetResult(error);
    internal Task<Exception?> WaitAsync(CancellationToken cancellationToken) => _completion.Task.WaitAsync(cancellationToken);
}

public sealed class SpruceException(int statusCode, string status, string? code, string body) : Exception($"Spruce {statusCode} {status}: {code ?? body}")
{
    public int StatusCode { get; } = statusCode;
    public string Status { get; } = status;
    public string? Code { get; } = code;
    public string Body { get; } = body;
}

public sealed class Deduper(int maxEntries = 65536, TimeSpan? ttl = null)
{
    private readonly int _max = maxEntries > 0 ? maxEntries : throw new ArgumentOutOfRangeException(nameof(maxEntries));
    private readonly TimeSpan _ttl = ttl ?? TimeSpan.FromMinutes(5);
    private readonly Dictionary<string, DateTimeOffset> _seen = [];
    private readonly Queue<(string Id, DateTimeOffset Until)> _order = [];
    private readonly object _lock = new();

    public bool Seen(string id)
    {
        lock (_lock)
        {
            var now = DateTimeOffset.UtcNow;
            if (_seen.TryGetValue(id, out var current) && current > now) return true;
            var until = now + _ttl;
            _seen[id] = until;
            _order.Enqueue((id, until));
            while (_order.Count > _max)
            {
                var old = _order.Dequeue();
                if (_seen.TryGetValue(old.Id, out current) && current == old.Until) _seen.Remove(old.Id);
            }
            return false;
        }
    }
}

public sealed partial class SpruceClient
{
    public Task SubscribeAsync(SubscribeOptions options, Func<Delivery, CancellationToken, Task> handler, CancellationToken cancellationToken = default) =>
        SubscribeAsync(options.Topic, options.Group, handler, cancellationToken, options.Concurrency, options.MaxPayloadBytes, options.Since, options.DrainTimeout);

    public async IAsyncEnumerable<ConsumableDelivery> ReadAllAsync(SubscribeOptions options, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<ConsumableDelivery>(Math.Max(1, options.Concurrency * 2));
        var subscription = SubscribeAsync(options, async (delivery, token) =>
        {
            var item = new ConsumableDelivery(delivery);
            await channel.Writer.WriteAsync(item, token);
            var failure = await item.WaitAsync(token);
            if (failure is not null) throw failure;
        }, cancellationToken);
        _ = subscription.ContinueWith(task => channel.Writer.TryComplete(task.Exception?.InnerException), TaskScheduler.Default);
        await foreach (var delivery in channel.Reader.ReadAllAsync(cancellationToken)) yield return delivery;
        await subscription;
    }

    public async Task<PublishResult> PublishWithRetryAsync(string topic, ReadOnlyMemory<byte> payload, PublishOptions options, RetryOptions? retry = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(options.ProducerId) || string.IsNullOrEmpty(options.IdempotencyKey)) throw new ArgumentException("Retry requires producer ID and idempotency key", nameof(options));
        retry ??= new();
        if (retry.MaxAttempts < 1) throw new ArgumentOutOfRangeException(nameof(retry));
        var delay = retry.MinBackoff ?? TimeSpan.FromMilliseconds(50);
        var maximum = retry.MaxBackoff ?? TimeSpan.FromSeconds(2);
        for (var attempt = 1; ; attempt++)
        {
            try { return await PublishAsync(topic, payload, options, cancellationToken); }
            catch (SpruceException ex) when (attempt < retry.MaxAttempts && ex.StatusCode is 408 or 429 or 503) { }
            catch (HttpRequestException) when (attempt < retry.MaxAttempts) { }
            await Task.Delay(delay, cancellationToken);
            delay = TimeSpan.FromMilliseconds(Math.Min(maximum.TotalMilliseconds, delay.TotalMilliseconds * 2));
        }
    }

    public async Task<BrokerStatus> GetStatusAsync(CancellationToken cancellationToken = default) =>
        await GetJsonAsync<BrokerStatus>("/v1/status", cancellationToken);

    public async Task CheckHealthAsync(CancellationToken cancellationToken = default) => await GetAsync("/health/ready", cancellationToken);

    public async Task<bool> IsMessageCachedAsync(string id, CancellationToken cancellationToken = default)
    {
        try { await GetAsync($"/v1/status/messages/{Uri.EscapeDataString(id)}", cancellationToken); return true; }
        catch (SpruceException ex) when (ex.StatusCode == 404) { return false; }
    }

    public async Task<string> GetMetricsAsync(CancellationToken cancellationToken = default)
    {
        using var response = await SendGetAsync("/metrics", cancellationToken);
        return await response.Content.ReadAsStringAsync(cancellationToken);
    }

    private async Task<T> GetJsonAsync<T>(string path, CancellationToken cancellationToken)
    {
        using var response = await SendGetAsync(path, cancellationToken);
        return (await response.Content.ReadFromJsonAsync<T>(cancellationToken))!;
    }
    private async Task GetAsync(string path, CancellationToken cancellationToken) { using var response = await SendGetAsync(path, cancellationToken); }
    private async Task<HttpResponseMessage> SendGetAsync(string path, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, _baseUrl + path); Authorize(request);
        var response = await SendAsync(request, HttpCompletionOption.ResponseContentRead, cancellationToken, "get");
        try { await EnsureSuccessAsync(response, cancellationToken); return response; } catch { response.Dispose(); throw; }
    }
}
