namespace SimpleMessageBroker.Client.Models;

/// <summary>
/// Represents a consumed message with byte[] payload.
/// </summary>
public class MqMessage
{
    public string Id { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public string? Key { get; set; }
    public int Partition { get; set; }
    public byte[] Payload { get; set; } = Array.Empty<byte>();
    public string ContentType { get; set; } = string.Empty;
    public Dictionary<string, string>? Headers { get; set; }
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// Result of a produce operation.
/// </summary>
public class ProduceResult
{
    public string MessageId { get; set; } = string.Empty;
    public int Partition { get; set; }
    public DateTime CreatedAt { get; set; }
}

/// <summary>
/// Result of a consume (pull) operation.
/// </summary>
public class ConsumeResult
{
    public List<MqMessage> Messages { get; set; } = new();
    public int Count { get; set; }
    public bool HasMore { get; set; }
}

/// <summary>
/// Result of a batch acknowledge operation.
/// </summary>
public class BatchAckResult
{
    public int Acknowledged { get; set; }
    public int Failed { get; set; }
}

/// <summary>
/// Input model for producing a single message.
/// </summary>
public class ProduceMessage
{
    public string Topic { get; set; } = string.Empty;
    public string? Key { get; set; }
    public byte[] Payload { get; set; } = Array.Empty<byte>();
    public string? ContentType { get; set; }
    public Dictionary<string, string>? Headers { get; set; }
    public int? TtlSeconds { get; set; }
}

/// <summary>
/// Context passed to IMessageHandler with message metadata.
/// </summary>
public class MessageContext
{
    public string MessageId { get; init; } = string.Empty;
    public string Topic { get; init; } = string.Empty;
    public string? Key { get; init; }
    public int Partition { get; init; }
    public Dictionary<string, string>? Headers { get; init; }
    public DateTime CreatedAt { get; init; }
}
