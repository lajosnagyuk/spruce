using System.Buffers.Binary;
using System.Net;
using System.Text;
using Spruce;

var failures = new List<string>();
await Run("invalid batch", RejectsInvalidBatch, failures);
await Run("batch v2 per-entry keys", BatchV2PerEntryKeys, failures);
await Run("adaptive gzip wire", AdaptiveGzipWire, failures);
await Run("adaptive zstd wire", AdaptiveZstdWire, failures);
await Run("batcher coalesces keys and skips queued cancellation", BatcherKeysAndCancellation, failures);
await Run("bearer token", SendsBearerToken, failures);
await Run("permanent subscription error", SurfacesPermanentSubscriptionErrors, failures);
await Run("transient subscription retry", RetriesTransientSubscriptionErrors, failures);
await Run("ack failure cancellation", AckFailureCancellation, failures);
await Run("terminal drain timeout", TerminalDrainTimeout, failures);
await Run("basic auth and structured error", BasicAuthAndStructuredError, failures);
await Run("deduper", DeduperBehavior, failures);
await Run("subscription duplicate suppression", SubscriptionDuplicateSuppression, failures);
await Run("retry diagnostics and telemetry", RetryDiagnosticsTelemetry, failures);
await Run("explicit stream completion", ExplicitStreamCompletion, failures);
await Run("key ordered subscription", KeyOrderedSubscription, failures);
await Run("bounded ordered completion window", BoundedOrderedCompletionWindow, failures);
if (failures.Count > 0) throw new Exception(string.Join(Environment.NewLine, failures));
Console.WriteLine("C# conformance passed");

static async Task Run(string name, Func<Task> test, List<string> failures)
{
    try { await test(); Console.WriteLine($"PASS {name}"); }
    catch (Exception ex) { failures.Add($"FAIL {name}:{Environment.NewLine}{ex}"); }
}

static async Task RejectsInvalidBatch()
{
    var client = new SpruceClient("http://spruce.invalid", new HttpClient(new StubHandler(_ => Accepted())));
    await Expect<ArgumentException>(() => client.PublishBatchAsync("t", Array.Empty<ReadOnlyMemory<byte>>()));
    var tooMany = Enumerable.Repeat<ReadOnlyMemory<byte>>(ReadOnlyMemory<byte>.Empty, 4097).ToArray();
    await Expect<ArgumentException>(() => client.PublishBatchAsync("t", tooMany));
    await Expect<ArgumentException>(() => client.PublishBatchAsync("t", [new byte[1]], new(Ack: "one-peer")));
}

static async Task BatchV2PerEntryKeys()
{
    byte[]? wire = null;
    string? version = null;
    var client = new SpruceClient("https://spruce.invalid", new HttpClient(new StubHandler(request =>
    {
        version = request.Headers.GetValues("Spruce-Batch-Version").Single();
        wire = request.Content!.ReadAsByteArrayAsync().GetAwaiter().GetResult();
        return new(HttpStatusCode.Accepted) { Content = new StringContent("{\"ids\":[\"1\",\"2\"]}") };
    })));
    await client.PublishBatchAsync("t", [new BatchEntry(new byte[] { 1 }, "a"), new BatchEntry(new byte[] { 2 }, "b")]);
    Assert(version == "2", "batch v2 header missing");
    Assert(wire is not null && wire.SequenceEqual(new byte[] { 0,1,(byte)'a',0,0,0,1,1, 0,1,(byte)'b',0,0,0,1,2 }), "unexpected batch v2 wire");
}

static async Task AdaptiveGzipWire()
{
    byte[]? wire = null;
    var client = new SpruceClient("https://spruce.invalid", new HttpClient(new StubHandler(request =>
    {
        wire = request.Content!.ReadAsByteArrayAsync().GetAwaiter().GetResult();
        return Accepted();
    })));
    var payload = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat("{\"event\":\"workspace.updated\",\"status\":\"ready\"}", 4096)));
    await client.PublishAsync("t", payload, new(Compression: "gzip"));
    Assert(wire is not null && wire.Length < payload.Length / 2, "gzip did not materially reduce the wire payload");
    Assert(wire!.AsSpan(0, 8).SequenceEqual(new byte[] { 0x89, (byte)'S', (byte)'P', (byte)'R', (byte)'U', (byte)'C', (byte)'E', 0x01 }), "compression envelope magic missing");
    await Expect<ArgumentException>(() => client.PublishAsync("t", payload, new(Compression: "brotli")));
}

static async Task AdaptiveZstdWire()
{
    byte[]? wire = null;
    var client = new SpruceClient("https://spruce.invalid", new HttpClient(new StubHandler(request =>
    {
        wire = request.Content!.ReadAsByteArrayAsync().GetAwaiter().GetResult();
        return Accepted();
    })));
    var payload = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat("{\"event\":\"workspace.updated\",\"status\":\"ready\"}", 4096)));
    await client.PublishAsync("t", payload, new(Compression: "zstd"));
    Assert(wire is not null && wire.Length < payload.Length / 2, "zstd did not materially reduce the wire payload");
    Assert(wire![8] == 2, "zstd compression envelope codec missing");
}

static async Task BatcherKeysAndCancellation()
{
    var requests = 0;
    byte[]? wire = null;
    var client = new SpruceClient("https://spruce.invalid", new HttpClient(new StubHandler(request =>
    {
        Interlocked.Increment(ref requests);
        wire = request.Content!.ReadAsByteArrayAsync().GetAwaiter().GetResult();
        return new(HttpStatusCode.Accepted) { Content = new StringContent("{\"ids\":[\"1\",\"2\"]}") };
    })));
    await using var batcher = new ProducerBatcher(client, new(MaxMessages: 2, MaxDelay: TimeSpan.FromSeconds(1)));
    var first = batcher.PublishAsync("t", new byte[] { 1 }, new(Key: "event-1"));
    var second = batcher.PublishAsync("t", new byte[] { 2 }, new(Key: "event-2"));
    await Task.WhenAll(first, second);
    Assert(requests == 1, $"unique keys did not coalesce: {requests}");
    Assert(wire is not null && Encoding.UTF8.GetString(wire).Contains("event-1") && Encoding.UTF8.GetString(wire).Contains("event-2"), "per-entry keys missing from batch");

    using var cancelled = new CancellationTokenSource();
    cancelled.Cancel();
    await Expect<OperationCanceledException>(() => batcher.PublishAsync("t", new byte[] { 3 }, new(Key: "cancelled"), cancelled.Token));
    Assert(requests == 1, "already-cancelled item reached the wire");
}

static async Task SendsBearerToken()
{
    string? authorization = null;
    var handler = new StubHandler(request =>
    {
        authorization = request.Headers.Authorization?.ToString();
        return Accepted();
    });
    var client = new SpruceClient("http://spruce.invalid", new HttpClient(handler), "secret", allowInsecureCredentials: true);
    await client.PublishAsync("t", Encoding.UTF8.GetBytes("payload"));
    Assert(authorization == "Bearer secret", $"unexpected authorization header: {authorization}");
}

static async Task SurfacesPermanentSubscriptionErrors()
{
    var client = new SpruceClient("http://spruce.invalid", new HttpClient(new StubHandler(_ => new(HttpStatusCode.Unauthorized))));
    await Expect<SpruceException>(() => client.SubscribeAsync("t", null, (_, _) => Task.CompletedTask));
}

static async Task RetriesTransientSubscriptionErrors()
{
    using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
    var calls = 0;
    var client = new SpruceClient("https://spruce.invalid", new HttpClient(new StubHandler(_ =>
    {
        if (Interlocked.Increment(ref calls) == 1) return new(HttpStatusCode.ServiceUnavailable) { Content = new StringContent("{\"error\":\"unavailable\"}") };
        cancellation.Cancel();
        return new(HttpStatusCode.OK) { Content = new StreamContent(Stream.Null) };
    })));
    try { await client.SubscribeAsync("t", null, (_, _) => Task.CompletedTask, cancellation.Token); }
    catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
    Assert(calls >= 2, $"transient subscription response was not retried: {calls}");
}

static async Task AckFailureCancellation()
{
    using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
    var ackCalls = 0;
    var handler = new StubHandler(request =>
    {
        if (request.RequestUri!.AbsolutePath == "/v1/deliveries/ack")
        {
            Interlocked.Increment(ref ackCalls);
            cancellation.Cancel();
            return new(HttpStatusCode.ServiceUnavailable);
        }
        return new(HttpStatusCode.OK) { Content = new StreamContent(Frame()) };
    });
    var client = new SpruceClient("http://spruce.invalid", new HttpClient(handler));
    await client.SubscribeAsync("t", null, (_, _) => Task.CompletedTask, cancellation.Token);
    Assert(ackCalls == 1, $"expected one failed ACK, got {ackCalls}");
}

static async Task TerminalDrainTimeout()
{
    var streams = 0;
    var handler = new StubHandler(_ =>
    {
        Interlocked.Increment(ref streams);
        return new(HttpStatusCode.OK) { Content = new StreamContent(Frame()) };
    });
    var client = new SpruceClient("http://spruce.invalid", new HttpClient(handler));
    var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    await Expect<TimeoutException>(() => client.SubscribeAsync("t", null, (_, _) => never.Task, drainTimeout: TimeSpan.FromMilliseconds(10)));
    never.SetResult();
    Assert(streams == 1, $"drain timeout reconnected {streams} times");
}

static async Task BasicAuthAndStructuredError()
{
    string? authorization = null;
    var client = new SpruceClient("https://spruce.invalid", new HttpClient(new StubHandler(request =>
    {
        authorization = request.Headers.Authorization?.ToString();
        return new(HttpStatusCode.Conflict) { Content = new StringContent("{\"error\":\"idempotency_conflict\"}") };
    })), username: "user", password: "pass");
    try { await client.PublishAsync("t", new byte[] { 1 }); }
    catch (SpruceException ex)
    {
        Assert(ex.StatusCode == 409 && ex.Code == "idempotency_conflict", $"unexpected structured error: {ex}");
        Assert(authorization == "Basic dXNlcjpwYXNz", $"unexpected basic header: {authorization}");
        return;
    }
    throw new Exception("expected SpruceException");
}

static Task DeduperBehavior()
{
    var deduper = new Deduper(2, TimeSpan.FromMinutes(1));
    Assert(!deduper.Seen("a"), "new ID was duplicate");
    Assert(deduper.Seen("a"), "existing ID was not duplicate");
    return Task.CompletedTask;
}

static async Task SubscriptionDuplicateSuppression()
{
    using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
    var calls = 0;
    var acks = 0;
    var nacks = 0;
    var stream = Frames(("d1", "m1", "same", (byte)1), ("d2", "m1", "same", (byte)1), ("d3", "m1", "same", (byte)1));
    var client = new SpruceClient("https://spruce.invalid", new HttpClient(new StubHandler(request =>
    {
        if (request.RequestUri!.AbsolutePath.EndsWith("/ack"))
        {
            if (Interlocked.Increment(ref acks) == 2) cancellation.Cancel();
            return new(HttpStatusCode.NoContent);
        }
        if (request.RequestUri.AbsolutePath.EndsWith("/nack")) { Interlocked.Increment(ref nacks); return new(HttpStatusCode.NoContent); }
        return new(HttpStatusCode.OK) { Content = new StreamContent(stream) };
    })));
    try
    {
        await client.SubscribeAsync(new SubscribeOptions("t", Concurrency: 1, PreserveKeyOrder: true, Deduper: new Deduper()), (_, _) =>
        {
            var attempt = Interlocked.Increment(ref calls);
            if (attempt == 1) throw new InvalidOperationException("retry me");
            return Task.CompletedTask;
        }, cancellation.Token);
    }
    catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
    Assert(calls == 2, $"expected failed and successful handler attempts only, got {calls}");
    Assert(nacks == 1, $"expected one NACK, got {nacks}");
    Assert(acks == 2, $"expected successful and suppressed duplicate ACKs, got {acks}");
}

static async Task RetryDiagnosticsTelemetry()
{
    var publishes = 0;
    var events = new List<ClientEvent>();
    var client = new SpruceClient("https://spruce.invalid", new HttpClient(new StubHandler(request =>
    {
        if (request.RequestUri!.AbsolutePath == "/v1/status")
            return new(HttpStatusCode.OK) { Content = new StringContent("{\"messages\":3,\"cache_accounted_bytes\":10,\"cache_limit_bytes\":20,\"peers\":2,\"consumers\":1,\"pending_deliveries\":0}") };
        if (Interlocked.Increment(ref publishes) == 1)
            return new(HttpStatusCode.ServiceUnavailable) { Content = new StringContent("{\"error\":\"peer_ack_unavailable\"}") };
        return Accepted();
    })))
    { OnEvent = value => events.Add(value) };
    var result = await client.PublishWithRetryAsync("t", new byte[] { 1 }, new(ProducerId: "p", IdempotencyKey: "1"), new(2, TimeSpan.FromMilliseconds(1)));
    var status = await client.GetStatusAsync();
    Assert(result.Id == "id" && publishes == 2 && status.Messages == 3 && events.Count == 3, $"retry/status/events mismatch: {publishes}/{status.Messages}/{events.Count}");
    Assert(events[0].StatusCode == 503 && events[0].Error is SpruceException, "failed attempt telemetry omitted its HTTP error");
    Assert(events[1].StatusCode == 202 && events[1].Error is null && events[2].StatusCode == 200 && events[2].Error is null, "successful telemetry was marked failed");
}

static async Task ExplicitStreamCompletion()
{
    using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
    var acks = 0;
    var client = new SpruceClient("https://spruce.invalid", new HttpClient(new StubHandler(request =>
    {
        if (request.RequestUri!.AbsolutePath == "/v1/deliveries/ack") { Interlocked.Increment(ref acks); cancellation.Cancel(); return new(HttpStatusCode.NoContent); }
        return new(HttpStatusCode.OK) { Content = new StreamContent(Frame()) };
    })));
    try
    {
        await foreach (var item in client.ReadAllAsync(new("t"), cancellation.Token))
        {
            Assert(acks == 0, "delivery ACKed before explicit completion");
            item.Complete();
        }
    }
    catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
    Assert(acks == 1, $"expected one ACK, got {acks}");
}

static async Task KeyOrderedSubscription()
{
    using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
    var completed = new List<byte>();
    var stream = Frames(("d1", "m1", "same", (byte)1), ("d2", "m2", "same", (byte)2));
    var client = new SpruceClient("https://spruce.invalid", new HttpClient(new StubHandler(request =>
    {
        if (request.RequestUri!.AbsolutePath.StartsWith("/v1/deliveries/")) return new(HttpStatusCode.NoContent);
        return new(HttpStatusCode.OK) { Content = new StreamContent(stream) };
    })));
    try
    {
        await client.SubscribeAsync(new SubscribeOptions("t", Concurrency: 2, PreserveKeyOrder: true), async (delivery, _) =>
        {
            if (delivery.Payload[0] == 1) await Task.Delay(50);
            lock (completed) completed.Add(delivery.Payload[0]);
            if (completed.Count == 2) cancellation.Cancel();
        }, cancellation.Token);
    }
    catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
    Assert(completed.SequenceEqual(new byte[] { 1, 2 }), $"same-key deliveries completed out of order: {string.Join(',', completed)}");
}

static async Task BoundedOrderedCompletionWindow()
{
    using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(2));
    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var started = 0;
    var stream = Frames(Enumerable.Range(0, 32).Select(i => ($"d{i}", $"m{i}", $"k{i}", (byte)i)).ToArray());
    var client = new SpruceClient("https://spruce.invalid", new HttpClient(new StubHandler(request =>
    {
        if (request.RequestUri!.AbsolutePath.StartsWith("/v1/deliveries/")) return new(HttpStatusCode.NoContent);
        return new(HttpStatusCode.OK) { Content = new StreamContent(stream) };
    })));
    var subscription = client.SubscribeAsync("t", null, async (delivery, _) =>
    {
        if (Interlocked.Increment(ref started) == 32) cancellation.Cancel();
        if (delivery.DeliveryId == "d0") await release.Task;
    }, cancellation.Token, concurrency: 2, drainTimeout: TimeSpan.FromMilliseconds(100));
    await Task.Delay(50);
    Assert(started <= 4, $"ordered completion window admitted {started} handlers with capacity 4");
    release.SetResult();
    try { await subscription; } catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
}

static Stream Frames(params (string Delivery, string Message, string Key, byte Payload)[] deliveries)
{
    var output = new MemoryStream();
    foreach (var item in deliveries)
    {
        var metadata = Encoding.UTF8.GetBytes($"{{\"delivery_id\":\"{item.Delivery}\",\"message_id\":\"{item.Message}\",\"topic\":\"t\",\"key\":\"{item.Key}\",\"created_at\":1,\"attempt\":1}}");
        var sizes = new byte[8];
        BinaryPrimitives.WriteUInt32BigEndian(sizes.AsSpan(0, 4), (uint)metadata.Length);
        BinaryPrimitives.WriteUInt32BigEndian(sizes.AsSpan(4, 4), 1);
        output.Write(sizes);
        output.Write(metadata);
        output.WriteByte(item.Payload);
    }
    output.Position = 0;
    return output;
}

static Stream Frame()
{
    var metadata = Encoding.UTF8.GetBytes("{\"delivery_id\":\"d\",\"message_id\":\"m\",\"topic\":\"t\",\"created_at\":1,\"attempt\":1}");
    var frame = new byte[8 + metadata.Length];
    BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(0, 4), (uint)metadata.Length);
    metadata.CopyTo(frame.AsSpan(8));
    return new MemoryStream(frame);
}

static HttpResponseMessage Accepted() => new(HttpStatusCode.Accepted)
{
    Content = new StringContent("{\"id\":\"id\",\"replicated\":false}", Encoding.UTF8, "application/json")
};

static async Task Expect<T>(Func<Task> action) where T : Exception
{
    try { await action(); }
    catch (T) { return; }
    throw new Exception($"expected {typeof(T).Name}");
}

static void Assert(bool condition, string message)
{
    if (!condition) throw new Exception(message);
}

sealed class StubHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
{
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        Task.FromResult(respond(request));
}
