using System.Collections.Concurrent;
using Spruce;

if (args.Length < 2) throw new ArgumentException("usage: Spruce.ClusterProbe URL TOKEN");
var topic = "csharp-probe-" + Guid.NewGuid().ToString("N");
var endpoint = new Uri(args[0], UriKind.Absolute);
var client = new SpruceClient(endpoint.ToString(), token: args[1], allowInsecureCredentials: endpoint.Scheme == Uri.UriSchemeHttp);
using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
var received = new ConcurrentDictionary<string, byte>();
var subscribed = client.SubscribeAsync(topic, "csharp", (delivery, _) =>
{
    received.TryAdd(System.Text.Encoding.UTF8.GetString(delivery.Payload), 0);
    if (received.Count == 100) timeout.Cancel();
    return Task.CompletedTask;
}, timeout.Token, concurrency: 16);
await Task.Delay(500);
var publishes = Enumerable.Range(0, 100).Select(i => client.PublishAsync(topic, System.Text.Encoding.UTF8.GetBytes(i.ToString()), cancellationToken: CancellationToken.None));
await Task.WhenAll(publishes);
try { await subscribed; } catch (OperationCanceledException) when (received.Count == 100) { }
if (received.Count != 100) throw new Exception($"expected 100 unique messages, received {received.Count}");
Console.WriteLine("C# live cluster probe passed: 100/100 unique group messages");
