using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using SimpleMessageBroker.Client;
using SimpleMessageBroker.Client.Extensions;
using SimpleMessageBroker.Client.Models;

// --- DI setup ---
var services = new ServiceCollection();

services.AddMessageQueueClient(options =>
{
    options.BaseAddress = "http://localhost:5292";
    // options.ApiKey = "your-api-key";
});

// Register the typed message handler
services.AddSingleton<IMessageHandler<OrderPayload>, OrderHandler>();

var serviceProvider = services.BuildServiceProvider();
var client = serviceProvider.GetRequiredService<IMessageQueueClient>();
var handler = serviceProvider.GetRequiredService<IMessageHandler<OrderPayload>>();

const string topic = "orders";
const string consumerGroup = "order-processors";
var consumerId = $"consumer-{Environment.MachineName}-{Environment.ProcessId}";

Console.WriteLine("=== SimpleMessageBroker Consumer Sample ===");
Console.WriteLine($"Topic          : {topic}");
Console.WriteLine($"Consumer group : {consumerGroup}");
Console.WriteLine($"Consumer id    : {consumerId}");
Console.WriteLine("Server         : http://localhost:5000");
Console.WriteLine("Press Ctrl+C to exit.\n");

try
{
    while (true)
    {
        var result = await client.ConsumeAsync(
            topic,
            consumerGroup,
            consumerId: consumerId,
            batchSize: 10,
            timeoutSeconds: 30);

        if (result.Messages.Count == 0)
        {
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] No messages, waiting...");
            continue;
        }

        Console.WriteLine($"\n[{DateTime.Now:HH:mm:ss}] Received {result.Messages.Count} message(s):");

        foreach (var msg in result.Messages)
        {
            try
            {
                // Deserialize the byte[] payload into a typed object
                var order = JsonSerializer.Deserialize<OrderPayload>(msg.Payload);
                var context = new MessageContext
                {
                    MessageId = msg.Id,
                    Topic = msg.Topic,
                    Key = msg.Key,
                    Partition = msg.Partition,
                    Headers = msg.Headers,
                    CreatedAt = msg.CreatedAt
                };

                await handler.HandleAsync(order!, context);

                // Acknowledge after successful processing
                await client.AcknowledgeAsync(msg.Id, consumerGroup, consumerId);
                Console.WriteLine($"  ✓ Acked  {msg.Id}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  ✗ Failed {msg.Id}: {ex.Message}");
                // Message will be redelivered after visibility timeout
            }
        }
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("\nConsumer stopped.");
}

// --- Payload model ---
record OrderPayload(string OrderId, string Product, int Quantity, DateTime Timestamp);

// --- Typed handler ---
class OrderHandler : IMessageHandler<OrderPayload>
{
    public Task HandleAsync(OrderPayload message, MessageContext context, CancellationToken cancellationToken = default)
    {
        Console.WriteLine(
            $"  >> Processing order {message.OrderId} " +
            $"| product={message.Product} qty={message.Quantity} " +
            $"| partition={context.Partition}");
        return Task.CompletedTask;
    }
}
