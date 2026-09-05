using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using SimpleMessageBroker.Client;
using SimpleMessageBroker.Client.Extensions;

// --- DI setup ---
var services = new ServiceCollection();

services.AddMessageQueueClient(options =>
{
    options.BaseAddress = "http://localhost:5292";
    // options.ApiKey = "your-api-key";
});

var serviceProvider = services.BuildServiceProvider();
var client = serviceProvider.GetRequiredService<IMessageQueueClient>();

const string topic = "orders";

Console.WriteLine("=== SimpleMessageBroker Producer Sample ===");
Console.WriteLine($"Target topic : {topic}");
Console.WriteLine("Server         : http://localhost:5000");
Console.WriteLine("Press Ctrl+C to exit.\n");

// Ensure topic exists (auto-created on first produce if server allows)
var sequence = 0;

try
{
    while (true)
    {
        sequence++;

        // Build a sample order payload
        var order = new
        {
            OrderId = Guid.NewGuid().ToString("N")[..8],
            Product = $"Product-{sequence}",
            Quantity = Random.Shared.Next(1, 100),
            Timestamp = DateTime.UtcNow
        };

        var payload = JsonSerializer.SerializeToUtf8Bytes(order);

        var result = await client.ProduceAsync(
            topic,
            payload,
            key: order.OrderId,
            contentType: "application/json",
            headers: new Dictionary<string, string>
            {
                ["source"] = "sample-producer",
                ["seq"] = sequence.ToString()
            });

        Console.WriteLine(
            $"[{DateTime.Now:HH:mm:ss}] Sent order {order.OrderId} " +
            $"| msg={result.MessageId} | partition={result.Partition}");

        await Task.Delay(TimeSpan.FromSeconds(2));
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("\nProducer stopped.");
}
