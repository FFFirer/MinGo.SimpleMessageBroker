using System.ComponentModel.DataAnnotations;

namespace SimpleMessageBroker.Server.DTOs;

public class ConsumeRequest
{
    [Required, MaxLength(100)]
    public string Topic { get; set; } = string.Empty;

    [Required, MaxLength(100)]
    public string ConsumerGroup { get; set; } = string.Empty;

    [MaxLength(200)]
    public string? ConsumerId { get; set; }

    [Range(1, 100)]
    public int BatchSize { get; set; } = 10;

    public int TimeoutSeconds { get; set; } = 30;
}

public class ConsumeResponse
{
    public List<ConsumedMessage> Messages { get; set; } = new();
    public int Count { get; set; }
    public bool HasMore { get; set; }
}

public class ConsumedMessage
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

public class BatchAckRequest
{
    [Required]
    public List<string> MessageIds { get; set; } = new();

    [Required, MaxLength(100)]
    public string ConsumerGroup { get; set; } = string.Empty;

    [MaxLength(200)]
    public string? ConsumerId { get; set; }
}

public class BatchAckResponse
{
    public int Acknowledged { get; set; }
    public int Failed { get; set; }
}
