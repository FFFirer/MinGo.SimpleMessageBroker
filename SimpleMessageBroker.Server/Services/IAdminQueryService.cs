using SimpleMessageBroker.Server.DTOs;

namespace SimpleMessageBroker.Server.Services;

public interface IAdminQueryService
{
    Task<DashboardStats> GetDashboardStatsAsync();
    Task<List<TopicListItem>> GetTopicsAsync();
    Task<TopicDetailInfo?> GetTopicDetailAsync(string topicName);
    Task<(List<MessageListItem> Messages, int Total)> GetMessagesAsync(string? topic, int page, int pageSize, string? status);
    Task<List<ConsumerGroupInfo>> GetConsumerGroupsAsync();
}

// --- DTOs ---

public class DashboardStats
{
    public int TotalTopics { get; set; }
    public int ActiveTopics { get; set; }
    public long TotalMessages { get; set; }
    public long PendingMessages { get; set; }
    public long ConsumedMessages { get; set; }
    public long ExpiredMessages { get; set; }
    public int TotalConsumerGroups { get; set; }
}

public class TopicListItem
{
    public string Name { get; set; } = string.Empty;
    public int PartitionCount { get; set; }
    public int DefaultTtlSeconds { get; set; }
    public DateTime CreatedAt { get; set; }
    public bool IsActive { get; set; }
    public long PendingMessages { get; set; }
    public long TotalMessages { get; set; }
    public int ConsumerGroupCount { get; set; }
}

public class TopicDetailInfo
{
    public string Name { get; set; } = string.Empty;
    public int PartitionCount { get; set; }
    public int DefaultTtlSeconds { get; set; }
    public DateTime CreatedAt { get; set; }
    public bool IsActive { get; set; }
    public long TotalMessages { get; set; }
    public long PendingMessages { get; set; }
    public long ConsumedMessages { get; set; }
    public long ExpiredMessages { get; set; }
    public List<PartitionDepthInfo> Partitions { get; set; } = new();
    public List<string> ConsumerGroups { get; set; } = new();
}

public class PartitionDepthInfo
{
    public int Partition { get; set; }
    public long Total { get; set; }
    public long Pending { get; set; }
}

public class MessageListItem
{
    public string Id { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public string? Key { get; set; }
    public int Partition { get; set; }
    public string ContentType { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    public DateTime? ExpiresAt { get; set; }
    public bool IsConsumed { get; set; }
    public string? ConsumerGroup { get; set; }
    public DateTime? ConsumedAt { get; set; }
    public string Status { get; set; } = string.Empty;
    public int PayloadSize { get; set; }
    public string? Headers { get; set; }
    public byte[] Payload { get; set; } = Array.Empty<byte>();
}

public class ConsumerGroupInfo
{
    public string ConsumerGroup { get; set; } = string.Empty;
    public List<ConsumerGroupTopicInfo> Topics { get; set; } = new();
}

public class ConsumerGroupTopicInfo
{
    public string Topic { get; set; } = string.Empty;
    public List<ConsumerOffsetInfo> Offsets { get; set; } = new();
}

public class ConsumerOffsetInfo
{
    public int Partition { get; set; }
    public long LastOffset { get; set; }
    public DateTime UpdatedAt { get; set; }
}
