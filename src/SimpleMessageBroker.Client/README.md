# SimpleMessageBroker.Client

C# Client SDK for [SimpleMessageBroker](https://github.com/FFFirer/MinGo.SimpleMessageBroker) — a lightweight single-node message queue middleware.

## Features

- **JSON over HTTP** — RESTful API communication, easy to integrate and debug
- **`byte[]` Payload** — serialization-agnostic; bring your own serializer
- **DI Integration** — one-liner `AddMessageQueueClient()` registration with `IHttpClientFactory` connection pooling
- **Exponential Backoff Retry** — automatic retry on HTTP 5xx, network timeout, and connection failures (no retry on 4xx)
- **Batch Operations** — produce and acknowledge messages in batches
- **Typed Message Handler** — `IMessageHandler<T>` for type-safe message consumption
- **Multi-TFM Support** — targets `net8.0` and `net10.0`

## Supported Frameworks

| TFM     | Status     |
|---------|------------|
| net8.0  | Compatible |
| net10.0 | Compatible |

## Dependencies

| Package                                        | net8.0   | net10.0  |
|------------------------------------------------|----------|----------|
| Microsoft.Extensions.DependencyInjection.Abstractions | >= 8.0.2 | >= 10.0.11 |
| Microsoft.Extensions.Http                      | >= 8.0.1 | >= 10.0.11 |

## Installation

```bash
dotnet add package SimpleMessageBroker.Client
```

## Quick Start

### 1. Register the client

```csharp
using SimpleMessageBroker.Client.Extensions;

builder.Services.AddMessageQueueClient(options =>
{
    options.BaseAddress = "http://localhost:5000";
    // options.ApiKey = "your-api-key";       // optional
    // options.DefaultTimeout = TimeSpan.FromSeconds(30);
    // options.MaxRetries = 3;
    // options.MaxConnectionsPerServer = 50;
});
```

### 2. Produce messages

```csharp
var client = serviceProvider.GetRequiredService<IMessageQueueClient>();

var payload = JsonSerializer.SerializeToUtf8Bytes(new { OrderId = "123", Amount = 99.99 });

var result = await client.ProduceAsync(
    topic: "orders",
    payload: payload,
    key: "user-123",
    contentType: "application/json",
    headers: new Dictionary<string, string>
    {
        ["source"] = "order-service"
    });

Console.WriteLine($"Message ID: {result.MessageId}, Partition: {result.Partition}");
```

### 3. Consume messages

```csharp
var result = await client.ConsumeAsync(
    topic: "orders",
    consumerGroup: "payment-service",
    consumerId: "payment-instance-1",
    batchSize: 10,
    timeoutSeconds: 30);

foreach (var msg in result.Messages)
{
    // Process the message...
    var order = JsonSerializer.Deserialize<MyOrder>(msg.Payload);

    // Acknowledge after successful processing
    await client.AcknowledgeAsync(msg.Id, "payment-service", "payment-instance-1");
}
```

### 4. Batch produce

```csharp
var messages = new List<ProduceMessage>
{
    new()
    {
        Topic = "orders",
        Payload = JsonSerializer.SerializeToUtf8Bytes(new { OrderId = "001" }),
        Key = "user-001",
        ContentType = "application/json"
    },
    new()
    {
        Topic = "orders",
        Payload = JsonSerializer.SerializeToUtf8Bytes(new { OrderId = "002" }),
        Key = "user-002",
        ContentType = "application/json"
    }
};

var results = await client.ProduceBatchAsync(messages);
```

## API Reference

### `IMessageQueueClient`

| Method | Description |
|--------|-------------|
| `ProduceAsync(topic, payload, key?, contentType?, headers?, ttlSeconds?)` | Produce a single message |
| `ProduceBatchAsync(messages)` | Produce a batch of messages |
| `ConsumeAsync(topic, consumerGroup, consumerId?, batchSize?, timeoutSeconds?)` | Pull messages from a topic |
| `AcknowledgeAsync(messageId, consumerGroup, consumerId?)` | Acknowledge a single message |
| `AcknowledgeBatchAsync(messageIds, consumerGroup, consumerId?)` | Acknowledge a batch of messages |
| `GetQueueDepthAsync(topic, consumerGroup?)` | Get queue depth for a topic |

### `IMessageHandler<T>`

Generic typed message handler for type-safe consumption:

```csharp
public class OrderHandler : IMessageHandler<OrderPayload>
{
    public Task HandleAsync(OrderPayload message, MessageContext context, CancellationToken ct = default)
    {
        Console.WriteLine($"Processing order {message.OrderId} from partition {context.Partition}");
        return Task.CompletedTask;
    }
}
```

### `IPayloadSerializer`

Serialization interface — implement your own (SDK does NOT include any built-in serializer):

```csharp
public interface IPayloadSerializer
{
    byte[] Serialize<T>(T obj);
    T Deserialize<T>(byte[] data);
}
```

## Configuration

`MessageQueueClientOptions`:

| Option | Default | Description |
|--------|---------|-------------|
| `BaseAddress` | `http://localhost:5000` | SimpleMessageBroker server URL |
| `ApiKey` | `null` | API key for authentication (sent as `X-Api-Key` header) |
| `DefaultTimeout` | 30s | HTTP request timeout |
| `MaxRetries` | 3 | Maximum retry attempts |
| `InitialBackoffMs` | 100 | Initial retry backoff (ms) |
| `MaxBackoffMs` | 5000 | Maximum retry backoff (ms) |
| `BackoffMultiplier` | 2.0 | Backoff multiplier |
| `MaxConnectionsPerServer` | 50 | HTTP connection pool size |

## Retry Policy

Built-in exponential backoff retry with the following behavior:

| Scenario | Behavior |
|----------|----------|
| HTTP 4xx | No retry — throw immediately |
| HTTP 5xx | Exponential backoff retry |
| Network timeout | Exponential backoff retry |
| Connection failure | Exponential backoff retry |

Backoff sequence (default): `100ms → 200ms → 400ms` (max 3 retries).

## License

MIT
