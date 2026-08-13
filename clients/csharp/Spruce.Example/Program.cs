using System.Text;
using Spruce;

var server = Environment.GetEnvironmentVariable("SPRUCE_URL") ?? "http://localhost:8080";
var token = Environment.GetEnvironmentVariable("SPRUCE_TOKEN");
var client = new SpruceClient(server, token: token);

if (args.FirstOrDefault() == "produce")
{
    var topic = args.ElementAtOrDefault(1) ?? "demo";
    var payload = Encoding.UTF8.GetBytes(args.ElementAtOrDefault(2) ?? "hello from csharp");
    var result = await client.PublishAsync(topic, payload);
    Console.WriteLine(result.Id);
    return;
}

var consumeTopic = args.ElementAtOrDefault(1) ?? "demo";
Console.WriteLine($"consuming {consumeTopic}");
await client.SubscribeAsync(consumeTopic, args.ElementAtOrDefault(2), (delivery, _) =>
{
    Console.WriteLine($"{delivery.MessageId} attempt={delivery.Attempt} {Encoding.UTF8.GetString(delivery.Payload)}");
    return Task.CompletedTask;
});
