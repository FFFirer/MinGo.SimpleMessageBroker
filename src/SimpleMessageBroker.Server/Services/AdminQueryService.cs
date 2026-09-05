using Microsoft.EntityFrameworkCore;
using SimpleMessageBroker.Server.Data;

namespace SimpleMessageBroker.Server.Services;

public class AdminQueryService : IAdminQueryService
{
    private readonly MessageQueueContext _context;

    public AdminQueryService(MessageQueueContext context)
    {
        _context = context;
    }

    public async Task<DashboardStats> GetDashboardStatsAsync()
    {
        var now = DateTime.UtcNow;

        var totalTopics = await _context.Topics.CountAsync();
        var activeTopics = await _context.Topics.CountAsync(t => t.IsActive);
        var totalMessages = await _context.Messages.LongCountAsync();
        var pendingMessages = await _context.Messages.LongCountAsync(m => !m.IsConsumed && (m.ExpiresAt == null || m.ExpiresAt > now));
        var consumedMessages = await _context.Messages.LongCountAsync(m => m.IsConsumed);
        var expiredMessages = await _context.Messages.LongCountAsync(m => m.ExpiresAt != null && m.ExpiresAt < now);
        var totalConsumerGroups = await _context.ConsumerOffsets
            .Select(o => o.ConsumerGroup)
            .Distinct()
            .CountAsync();

        return new DashboardStats
        {
            TotalTopics = totalTopics,
            ActiveTopics = activeTopics,
            TotalMessages = totalMessages,
            PendingMessages = pendingMessages,
            ConsumedMessages = consumedMessages,
            ExpiredMessages = expiredMessages,
            TotalConsumerGroups = totalConsumerGroups
        };
    }

    public async Task<List<TopicListItem>> GetTopicsAsync()
    {
        var now = DateTime.UtcNow;

        var topics = await _context.Topics
            .OrderBy(t => t.Name)
            .ToListAsync();

        var result = new List<TopicListItem>();
        foreach (var topic in topics)
        {
            var totalMessages = await _context.Messages.LongCountAsync(m => m.Topic == topic.Name);
            var pendingMessages = await _context.Messages.LongCountAsync(m => m.Topic == topic.Name && !m.IsConsumed && (m.ExpiresAt == null || m.ExpiresAt > now));
            var consumerGroupCount = await _context.ConsumerOffsets
                .Where(o => o.Topic == topic.Name)
                .Select(o => o.ConsumerGroup)
                .Distinct()
                .CountAsync();

            result.Add(new TopicListItem
            {
                Name = topic.Name,
                PartitionCount = topic.PartitionCount,
                DefaultTtlSeconds = topic.DefaultTtlSeconds,
                CreatedAt = topic.CreatedAt,
                IsActive = topic.IsActive,
                TotalMessages = totalMessages,
                PendingMessages = pendingMessages,
                ConsumerGroupCount = consumerGroupCount
            });
        }

        return result;
    }

    public async Task<TopicDetailInfo?> GetTopicDetailAsync(string topicName)
    {
        var topic = await _context.Topics.FirstOrDefaultAsync(t => t.Name == topicName);
        if (topic == null) return null;

        var now = DateTime.UtcNow;

        var totalMessages = await _context.Messages.LongCountAsync(m => m.Topic == topicName);
        var pendingMessages = await _context.Messages.LongCountAsync(m => m.Topic == topicName && !m.IsConsumed && (m.ExpiresAt == null || m.ExpiresAt > now));
        var consumedMessages = await _context.Messages.LongCountAsync(m => m.Topic == topicName && m.IsConsumed);
        var expiredMessages = await _context.Messages.LongCountAsync(m => m.Topic == topicName && m.ExpiresAt != null && m.ExpiresAt < now);

        var partitions = new List<PartitionDepthInfo>();
        for (int i = 0; i < topic.PartitionCount; i++)
        {
            var partition = i;
            var total = await _context.Messages.LongCountAsync(m => m.Topic == topicName && m.Partition == partition);
            var pending = await _context.Messages.LongCountAsync(m => m.Topic == topicName && m.Partition == partition && !m.IsConsumed && (m.ExpiresAt == null || m.ExpiresAt > now));
            partitions.Add(new PartitionDepthInfo { Partition = i, Total = total, Pending = pending });
        }

        var consumerGroups = await _context.ConsumerOffsets
            .Where(o => o.Topic == topicName)
            .Select(o => o.ConsumerGroup)
            .Distinct()
            .ToListAsync();

        return new TopicDetailInfo
        {
            Name = topic.Name,
            PartitionCount = topic.PartitionCount,
            DefaultTtlSeconds = topic.DefaultTtlSeconds,
            CreatedAt = topic.CreatedAt,
            IsActive = topic.IsActive,
            TotalMessages = totalMessages,
            PendingMessages = pendingMessages,
            ConsumedMessages = consumedMessages,
            ExpiredMessages = expiredMessages,
            Partitions = partitions,
            ConsumerGroups = consumerGroups
        };
    }

    public async Task<(List<MessageListItem> Messages, int Total)> GetMessagesAsync(string? topic, int page, int pageSize, string? status)
    {
        var now = DateTime.UtcNow;
        var query = _context.Messages.AsQueryable();

        if (!string.IsNullOrEmpty(topic))
            query = query.Where(m => m.Topic == topic);

        if (status == "pending")
            query = query.Where(m => !m.IsConsumed && (m.ExpiresAt == null || m.ExpiresAt > now));
        else if (status == "consumed")
            query = query.Where(m => m.IsConsumed);
        else if (status == "expired")
            query = query.Where(m => m.ExpiresAt != null && m.ExpiresAt < now);

        var total = await query.CountAsync();

        var messages = await query
            .OrderByDescending(m => m.CreatedAt)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();

        var items = messages.Select(m =>
        {
            var isExpired = m.ExpiresAt != null && m.ExpiresAt < now;
            var msgStatus = isExpired ? "expired" : (m.IsConsumed ? "consumed" : "pending");
            return new MessageListItem
            {
                Id = m.Id,
                Topic = m.Topic,
                Key = m.Key,
                Partition = m.Partition,
                ContentType = m.ContentType,
                CreatedAt = m.CreatedAt,
                ExpiresAt = m.ExpiresAt,
                IsConsumed = m.IsConsumed,
                ConsumerGroup = m.ConsumerGroup,
                ConsumedAt = m.ConsumedAt,
                Status = msgStatus,
                PayloadSize = m.Payload.Length,
                Headers = m.Headers,
                Payload = m.Payload
            };
        }).ToList();

        return (items, total);
    }

    public async Task<List<ConsumerGroupInfo>> GetConsumerGroupsAsync()
    {
        var offsets = await _context.ConsumerOffsets
            .OrderBy(o => o.Topic)
            .ThenBy(o => o.Partition)
            .ToListAsync();

        // Pre-load topic partition counts
        var topicNames = offsets.Select(o => o.Topic).Distinct().ToList();
        var topicPartitions = await _context.Topics
            .Where(t => topicNames.Contains(t.Name))
            .ToDictionaryAsync(t => t.Name, t => t.PartitionCount);

        // Load consumers for all groups
        var groupNames = offsets.Select(o => o.ConsumerGroup).Distinct().ToList();
        var consumers = await _context.Consumers
            .Where(c => groupNames.Contains(c.ConsumerGroup))
            .ToListAsync();

        // Load last consumer per partition from Messages table
        var partitionConsumers = await _context.Messages
            .Where(m => !string.IsNullOrEmpty(m.ConsumerId) && m.IsConsumed)
            .GroupBy(m => new { m.Topic, m.ConsumerGroup, m.Partition })
            .Select(g => new
            {
                g.Key.Topic,
                g.Key.ConsumerGroup,
                g.Key.Partition,
                ConsumerId = g.OrderByDescending(m => m.ConsumedAt).First().ConsumerId
            })
            .ToListAsync();

        var partitionConsumerMap = partitionConsumers
            .ToDictionary(
                pc => (pc.Topic, pc.ConsumerGroup, pc.Partition),
                pc => pc.ConsumerId);

        var aliveThreshold = DateTime.UtcNow.AddMinutes(-5);

        // Build a lookup of consumer heartbeat info by ID
        var consumerHeartbeatMap = consumers
            .ToDictionary(c => c.Id, c => new { c.LastHeartbeat, c.CreatedAt });

        return offsets
            .GroupBy(o => o.ConsumerGroup)
            .Select(g =>
            {
                // Derive unique consumer IDs from partition data (Messages table)
                var consumerIdsInPartitions = g
                    .Select(o => partitionConsumerMap.GetValueOrDefault((o.Topic, o.ConsumerGroup, o.Partition)))
                    .Where(id => id != null)
                    .Distinct()
                    .ToList();

                // Build partition assignment per consumer
                var consumerPartitions = new Dictionary<string, List<int>>();
                foreach (var offset in g)
                {
                    var key = (offset.Topic, offset.ConsumerGroup, offset.Partition);
                    if (partitionConsumerMap.TryGetValue(key, out var consumerId) && consumerId != null)
                    {
                        if (!consumerPartitions.ContainsKey(consumerId))
                            consumerPartitions[consumerId] = new List<int>();
                        consumerPartitions[consumerId].Add(offset.Partition);
                    }
                }

                // Build consumer infos from Messages-derived IDs, enriched with heartbeat data
                var consumerInfos = consumerIdsInPartitions.Select(id =>
                {
                    var hasHeartbeat = consumerHeartbeatMap.TryGetValue(id!, out var hb);
                    return new ConsumerInfo
                    {
                        ConsumerId = id!,
                        LastHeartbeat = hasHeartbeat ? hb!.LastHeartbeat : DateTime.MinValue,
                        CreatedAt = hasHeartbeat ? hb!.CreatedAt : DateTime.MinValue,
                        IsAlive = hasHeartbeat && hb!.LastHeartbeat > aliveThreshold,
                        Partitions = consumerPartitions.GetValueOrDefault(id!, new List<int>())
                    };
                }).OrderBy(c => c.ConsumerId).ToList();

                return new ConsumerGroupInfo
                {
                    ConsumerGroup = g.Key,
                    Consumers = consumerInfos,
                    Topics = g.GroupBy(o => o.Topic)
                        .Select(tg => new ConsumerGroupTopicInfo
                        {
                            Topic = tg.Key,
                            PartitionCount = topicPartitions.GetValueOrDefault(tg.Key, tg.Select(o => o.Partition).Max() + 1),
                            Offsets = tg.Select(o => new ConsumerOffsetInfo
                            {
                                Partition = o.Partition,
                                LastOffset = o.LastOffset,
                                UpdatedAt = o.UpdatedAt,
                                ConsumerId = partitionConsumerMap.GetValueOrDefault((o.Topic, o.ConsumerGroup, o.Partition))
                            }).ToList()
                        }).ToList()
                };
            }).ToList();
    }
}
