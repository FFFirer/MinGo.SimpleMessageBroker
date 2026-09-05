using System.ComponentModel.DataAnnotations;

namespace SimpleMessageBroker.Server.DTOs;

public class CreateTopicRequest
{
    [Required, MaxLength(100)]
    public string Name { get; set; } = string.Empty;

    [Range(1, 1000)]
    public int PartitionCount { get; set; } = 10;

    [Range(0, int.MaxValue)]
    public int DefaultTtlSeconds { get; set; } = 86400;
}

public class TopicResponse
{
    public string Name { get; set; } = string.Empty;
    public int PartitionCount { get; set; }
    public int DefaultTtlSeconds { get; set; }
    public DateTime CreatedAt { get; set; }
}

public class QueueDepthResponse
{
    public string Topic { get; set; } = string.Empty;
    public string? ConsumerGroup { get; set; }
    public long Depth { get; set; }
    public List<PartitionDepth> Partitions { get; set; } = new();
}

public class PartitionDepth
{
    public int Partition { get; set; }
    public long Depth { get; set; }
}

public class CleanupResponse
{
    public int DeletedExpired { get; set; }
    public int DeletedConsumed { get; set; }
    public int TotalDeleted { get; set; }
}
