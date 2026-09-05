using System.ComponentModel.DataAnnotations;

namespace SimpleMessageBroker.Server.DTOs;

public class ProduceRequest
{
    [Required, MaxLength(100)]
    public string Topic { get; set; } = string.Empty;

    [MaxLength(200)]
    public string? Key { get; set; }

    [Required]
    public byte[] Payload { get; set; } = Array.Empty<byte>();

    [MaxLength(100)]
    public string? ContentType { get; set; }

    public Dictionary<string, string>? Headers { get; set; }

    public int? TtlSeconds { get; set; }
}

public class BatchProduceRequest
{
    [Required]
    public List<ProduceRequest> Messages { get; set; } = new();
}

public class ProduceResponse
{
    public string MessageId { get; set; } = string.Empty;
    public int Partition { get; set; }
    public DateTime CreatedAt { get; set; }
}

public class BatchProduceResponse
{
    public List<ProduceResponse> Results { get; set; } = new();
    public int TotalCount { get; set; }
}
