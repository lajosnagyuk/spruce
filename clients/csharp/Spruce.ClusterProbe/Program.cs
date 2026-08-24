using System.Collections.Concurrent;
using Spruce;

if (args.Length < 2) throw new ArgumentException("usage: Spruce.ClusterProbe URL TOKEN");
var topic = "csharp-probe-" + Guid.NewGuid().ToString("N");
var endpoint = new Uri(args[0], UriKind.Absolute);
var client = new SpruceClient(endpoint.ToString(), token: args[1], allowInsecureCredentials: endpoint.Scheme == Uri.UriSchemeHttp);
using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
var received = new ConcurrentDictionary<string, byte>();
var expected = Enumerable.Range(0, 100).Select(i => $"{i}:" + new string((char)('a' + i % 26), 4096)).ToHashSet();
var subscribed = client.SubscribeAsync(topic, "csharp", (delivery, _) =>
{
    received.TryAdd(System.Text.Encoding.UTF8.GetString(delivery.Payload), 0);
    if (received.Count == 100) timeout.Cancel();
    return Task.CompletedTask;
}, timeout.Token, concurrency: 16);
await Task.Delay(500);
var publishes = expected.Select((value, i) => client.PublishAsync(topic, System.Text.Encoding.UTF8.GetBytes(value), new(Compression: (i % 3) switch { 1 => "gzip", 2 => "zstd", _ => null }), CancellationToken.None));
await Task.WhenAll(publishes);
try { await subscribed; } catch (OperationCanceledException) when (received.Count == 100) { }
if (!received.Keys.ToHashSet().SetEquals(expected)) throw new Exception($"expected 100 exact messages, received {received.Count}");
Console.WriteLine("C# live cluster probe passed: 100/100 exact raw, gzip, and zstd messages");
