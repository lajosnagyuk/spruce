using System.Net;
using System.Net.Http.Json;
using Spruce;

var requests = 0;
var messages = 0;
var handler = new StubHandler(async request =>
{
    Interlocked.Increment(ref requests);
    var body = await request.Content!.ReadAsByteArrayAsync();
    var offset = 0;
    var ids = new List<string>();
    while (offset < body.Length)
    {
        var size = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(body, offset));
        offset += 4 + size;
        ids.Add($"id-{Interlocked.Increment(ref messages)}");
    }
    return new HttpResponseMessage(HttpStatusCode.Accepted) { Content = JsonContent.Create(new { ids }) };
});
using var http = new HttpClient(handler);
var client = new SpruceClient("https://spruce.invalid", http);
await using (var batcher = new ProducerBatcher(client, new(MaxMessages: 16, MaxDelay: TimeSpan.FromSeconds(1))))
{
    var payload = new byte[] { 1, 2, 3 };
    var tasks = Enumerable.Range(0, 16).Select(_ => batcher.PublishAsync("topic", payload)).ToArray();
    payload[0] = 9;
    await Task.WhenAll(tasks);
    if (requests != 1 || messages != 16) throw new Exception($"count flush failed: requests={requests} messages={messages}");
}

requests = 0;
await using (var batcher = new ProducerBatcher(client, new(MaxDelay: TimeSpan.FromMilliseconds(10))))
{
    var started = DateTime.UtcNow;
    await batcher.PublishAsync("timer", new byte[] { 1 });
    if (DateTime.UtcNow - started < TimeSpan.FromMilliseconds(5)) throw new Exception("timer flushed too early");
    await batcher.FlushAsync();
}

var failedClient = new SpruceClient("https://spruce.invalid", new HttpClient(new StubHandler(_ => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable) { Content = JsonContent.Create(new { error = "unavailable" }) }))));
await using (var batcher = new ProducerBatcher(failedClient, new(MaxDelay: TimeSpan.FromSeconds(1))))
{
    var publish = batcher.PublishAsync("failure", new byte[] { 1 });
    try { await batcher.FlushAsync(); throw new Exception("flush hid batch failure"); } catch (SpruceException) { }
    try { await publish; throw new Exception("publish hid batch failure"); } catch (SpruceException) { }
}
var disposeBatcher = new ProducerBatcher(failedClient, new(MaxDelay: TimeSpan.FromSeconds(30)));
var disposePublish = disposeBatcher.PublishAsync("dispose-failure", new byte[] { 1 });
try { await disposeBatcher.DisposeAsync(); throw new Exception("dispose hid batch failure"); } catch (SpruceException) { }
try { await disposePublish; throw new Exception("dispose stranded publish failure"); } catch (SpruceException) { }
Console.WriteLine("C# producer batcher conformance passed");

sealed class StubHandler(Func<HttpRequestMessage, Task<HttpResponseMessage>> responder) : HttpMessageHandler
{
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) => responder(request);
}
