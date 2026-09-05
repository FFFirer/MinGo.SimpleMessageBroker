using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using SimpleMessageBroker.Server.Configuration;
using SimpleMessageBroker.Server.Data;
using SimpleMessageBroker.Server.DTOs;
using SimpleMessageBroker.Server.Models;

namespace SimpleMessageBroker.Server.Services;

public class MessageService : IMessageService
{
    private readonly MessageQueueContext _context;
    private readonly IPartitionRouter _router;
    private readonly ILogger<MessageService> _logger;
    private readonly MessageQueueOptions _options;

    public MessageService(
        MessageQueueContext context,
        IPartitionRouter router,
        ILogger<MessageService> logger,
        MessageQueueOptions options)
    {
        _context = context;
        _router = router;
        _logger = logger;
        _options = options;
    }

    public async Task<ProduceResponse> ProduceAsync(ProduceRequest request)
    {
        var topic = await GetOrCreateTopicAsync(request.Topic, request.TtlSeconds);

        var partition = _router.GetPartition(request.Topic, request.Key, topic.PartitionCount);

        var message = new Message
        {
            Id = Guid.NewGuid().ToString(),
            Topic = request.Topic,
            Key = request.Key,
            Partition = partition,
            Payload = request.Payload,
            ContentType = request.ContentType ?? "application/octet-stream",
            Headers = request.Headers != null ? JsonSerializer.Serialize(request.Headers) : null,
            CreatedAt = DateTime.UtcNow,
            ExpiresAt = CalculateExpiresAt(request.TtlSeconds, topic.DefaultTtlSeconds),
        };

        _context.Messages.Add(message);
        await _context.SaveChangesAsync();

        _logger.LogInformation(
            "Message produced: {MessageId}, Topic: {Topic}, Partition: {Partition}",
            message.Id, request.Topic, partition);

        return new ProduceResponse
        {
            MessageId = message.Id,
            Partition = partition,
            CreatedAt = message.CreatedAt
        };
    }

    public async Task<BatchProduceResponse> ProduceBatchAsync(IEnumerable<ProduceRequest> requests)
    {
        var results = new List<ProduceResponse>();
        foreach (var request in requests)
        {
            var result = await ProduceAsync(request);
            results.Add(result);
        }

        return new BatchProduceResponse
        {
            Results = results,
            TotalCount = results.Count
        };
    }

    public async Task<ConsumeResponse> ConsumeAsync(ConsumeRequest request)
    {
        // Auto-recover: create topic if it doesn't exist (self-recovery)
        var topic = await GetOrCreateTopicAsync(request.Topic, null);

        // Register or update consumer heartbeat
        if (!string.IsNullOrEmpty(request.ConsumerId))
        {
            await RegisterConsumerAsync(request.ConsumerId, request.ConsumerGroup);
        }

        // Get or initialize consumer offsets
        var offsets = await GetOrCreateConsumerOffsetsAsync(request.Topic, request.ConsumerGroup, topic.PartitionCount);

        var now = DateTime.UtcNow;

        // Query messages: not consumed, not expired, respecting offsets
        var messages = await _context.Messages
            .Where(m => m.Topic == request.Topic)
            .Where(m => !m.IsConsumed)
            .Where(m => m.ExpiresAt == null || m.ExpiresAt > now)
            .OrderBy(m => m.CreatedAt)
            .Take(request.BatchSize)
            .ToListAsync();

        if (messages.Count == 0)
        {
            return new ConsumeResponse
            {
                Messages = new List<ConsumedMessage>(),
                Count = 0,
                HasMore = false
            };
        }

        // Mark messages as consumed and update offsets
        foreach (var msg in messages)
        {
            msg.IsConsumed = true;
            msg.ConsumerGroup = request.ConsumerGroup;
            msg.ConsumerId = request.ConsumerId;
            msg.ConsumedAt = now;

            // Update offset for the partition (sequential counter)
            var offset = offsets.First(o => o.Partition == msg.Partition);
            offset.LastOffset += 1;
            offset.UpdatedAt = now;
        }

        await _context.SaveChangesAsync();

        var consumedMessages = messages.Select(m => new ConsumedMessage
        {
            Id = m.Id,
            Topic = m.Topic,
            Key = m.Key,
            Partition = m.Partition,
            Payload = m.Payload,
            ContentType = m.ContentType,
            Headers = m.Headers != null ? JsonSerializer.Deserialize<Dictionary<string, string>>(m.Headers) : null,
            CreatedAt = m.CreatedAt
        }).ToList();

        // Check if more messages are available
        var hasMore = await _context.Messages
            .AnyAsync(m => m.Topic == request.Topic
                && !m.IsConsumed
                && (m.ExpiresAt == null || m.ExpiresAt > DateTime.UtcNow));

        _logger.LogInformation(
            "Messages consumed: {Count} from Topic: {Topic}, Group: {Group}",
            consumedMessages.Count, request.Topic, request.ConsumerGroup);

        return new ConsumeResponse
        {
            Messages = consumedMessages,
            Count = consumedMessages.Count,
            HasMore = hasMore
        };
    }

    public async Task<bool> AckMessageAsync(string messageId, string consumerGroup, string? consumerId)
    {
        var message = await _context.Messages.FirstOrDefaultAsync(m => m.Id == messageId);
        if (message == null)
            return false;

        if (message.ConsumerGroup != consumerGroup)
            return false;

        // Already acknowledged
        _logger.LogInformation("Message acknowledged: {MessageId} by group {Group}", messageId, consumerGroup);
        return true;
    }

    public async Task<BatchAckResponse> AckBatchAsync(IEnumerable<string> messageIds, string consumerGroup, string? consumerId)
    {
        var acknowledged = 0;
        var failed = 0;

        foreach (var messageId in messageIds)
        {
            var result = await AckMessageAsync(messageId, consumerGroup, consumerId);
            if (result) acknowledged++;
            else failed++;
        }

        return new BatchAckResponse { Acknowledged = acknowledged, Failed = failed };
    }

    public async Task<CleanupResponse> CleanupAsync()
    {
        var now = DateTime.UtcNow;

        // Delete expired messages
        var expiredMessages = await _context.Messages
            .Where(m => m.ExpiresAt != null && m.ExpiresAt < now)
            .Take(_options.CleanupBatchSize)
            .ToListAsync();

        if (expiredMessages.Count > 0)
            _context.Messages.RemoveRange(expiredMessages);

        // Delete old consumed messages
        var retainThreshold = now.AddMinutes(-_options.RetainConsumedMinutes);
        var consumedMessages = await _context.Messages
            .Where(m => m.IsConsumed && m.ConsumedAt != null && m.ConsumedAt < retainThreshold)
            .Take(_options.CleanupBatchSize)
            .ToListAsync();

        if (consumedMessages.Count > 0)
            _context.Messages.RemoveRange(consumedMessages);

        await _context.SaveChangesAsync();

        var response = new CleanupResponse
        {
            DeletedExpired = expiredMessages.Count,
            DeletedConsumed = consumedMessages.Count,
            TotalDeleted = expiredMessages.Count + consumedMessages.Count
        };

        if (response.TotalDeleted > 0)
            _logger.LogInformation("Cleanup completed: {Total} messages deleted (expired: {Expired}, consumed: {Consumed})",
                response.TotalDeleted, response.DeletedExpired, response.DeletedConsumed);

        return response;
    }

    public async Task<QueueDepthResponse> GetQueueDepthAsync(string topic, string? consumerGroup)
    {
        var topicEntity = await _context.Topics.FirstOrDefaultAsync(t => t.Name == topic);
        if (topicEntity == null)
            throw new InvalidOperationException($"Topic '{topic}' not found");

        var baseQuery = _context.Messages
            .Where(m => m.Topic == topic && !m.IsConsumed)
            .Where(m => m.ExpiresAt == null || m.ExpiresAt > DateTime.UtcNow);

        var totalDepth = await baseQuery.LongCountAsync();

        var partitionDepths = new List<PartitionDepth>();
        for (int i = 0; i < topicEntity.PartitionCount; i++)
        {
            var partition = i;
            var depth = await baseQuery
                .Where(m => m.Partition == partition)
                .LongCountAsync();

            partitionDepths.Add(new PartitionDepth { Partition = i, Depth = depth });
        }

        return new QueueDepthResponse
        {
            Topic = topic,
            ConsumerGroup = consumerGroup,
            Depth = totalDepth,
            Partitions = partitionDepths
        };
    }

    private async Task<Topic> GetOrCreateTopicAsync(string topicName, int? requestTtlSeconds)
    {
        var topic = await _context.Topics.FirstOrDefaultAsync(t => t.Name == topicName);
        if (topic != null) return topic;

        topic = new Topic
        {
            Name = topicName,
            PartitionCount = _options.DefaultPartitionCount,
            DefaultTtlSeconds = requestTtlSeconds ?? _options.DefaultTtlSeconds,
            CreatedAt = DateTime.UtcNow,
            IsActive = true
        };

        _context.Topics.Add(topic);
        await _context.SaveChangesAsync();

        _logger.LogInformation("Topic auto-created: {Topic}, Partitions: {Partitions}", topic.Name, topic.PartitionCount);
        return topic;
    }

    private async Task<List<ConsumerOffset>> GetOrCreateConsumerOffsetsAsync(string topic, string consumerGroup, int partitionCount)
    {
        var offsets = await _context.ConsumerOffsets
            .Where(o => o.Topic == topic && o.ConsumerGroup == consumerGroup)
            .ToListAsync();

        if (offsets.Count > 0) return offsets;

        var newOffsets = new List<ConsumerOffset>();
        for (int i = 0; i < partitionCount; i++)
        {
            newOffsets.Add(new ConsumerOffset
            {
                Topic = topic,
                ConsumerGroup = consumerGroup,
                Partition = i,
                LastOffset = 0,
                UpdatedAt = DateTime.UtcNow
            });
        }

        await _context.ConsumerOffsets.AddRangeAsync(newOffsets);
        await _context.SaveChangesAsync();
        return newOffsets;
    }

    private async Task RegisterConsumerAsync(string consumerId, string consumerGroup)
    {
        var consumer = await _context.Consumers.FindAsync(consumerId);
        if (consumer != null)
        {
            consumer.LastHeartbeat = DateTime.UtcNow;
        }
        else
        {
            _context.Consumers.Add(new Models.Consumer
            {
                Id = consumerId,
                ConsumerGroup = consumerGroup,
                LastHeartbeat = DateTime.UtcNow,
                CreatedAt = DateTime.UtcNow
            });
        }

        await _context.SaveChangesAsync();
    }

    private static DateTime? CalculateExpiresAt(int? requestTtlSeconds, int defaultTtlSeconds)
    {
        var ttl = requestTtlSeconds ?? defaultTtlSeconds;
        // 0 or -1 means never expire
        if (ttl <= 0) return null;
        return DateTime.UtcNow.AddSeconds(ttl);
    }

    public async Task<DeleteTopicResponse> DeleteTopicAsync(string topicName)
    {
        var topic = await _context.Topics.FirstOrDefaultAsync(t => t.Name == topicName);
        if (topic == null)
            throw new InvalidOperationException($"Topic '{topicName}' not found");

        // Delete related messages
        var messages = await _context.Messages
            .Where(m => m.Topic == topicName)
            .ToListAsync();
        if (messages.Count > 0)
            _context.Messages.RemoveRange(messages);

        // Delete related consumer offsets
        var offsets = await _context.ConsumerOffsets
            .Where(o => o.Topic == topicName)
            .ToListAsync();
        if (offsets.Count > 0)
            _context.ConsumerOffsets.RemoveRange(offsets);

        // Delete consumers that only belonged to this topic's consumer groups
        var affectedGroups = offsets.Select(o => o.ConsumerGroup).Distinct().ToList();
        var consumers = await _context.Consumers
            .Where(c => affectedGroups.Contains(c.ConsumerGroup))
            .ToListAsync();
        // Only delete consumers whose all groups are being removed
        var remainingOffsets = await _context.ConsumerOffsets
            .Where(o => affectedGroups.Contains(o.ConsumerGroup) && o.Topic != topicName)
            .Select(o => o.ConsumerGroup)
            .Distinct()
            .ToListAsync();
        var deletableGroups = affectedGroups.Except(remainingOffsets).ToHashSet();
        var deletableConsumers = consumers.Where(c => deletableGroups.Contains(c.ConsumerGroup)).ToList();
        if (deletableConsumers.Count > 0)
            _context.Consumers.RemoveRange(deletableConsumers);

        // Delete the topic itself
        _context.Topics.Remove(topic);

        await _context.SaveChangesAsync();

        _logger.LogInformation(
            "Topic deleted: {Topic}, messages: {Messages}, offsets: {Offsets}, consumers: {Consumers}",
            topicName, messages.Count, offsets.Count, deletableConsumers.Count);

        return new DeleteTopicResponse
        {
            Topic = topicName,
            DeletedMessages = messages.Count,
            DeletedConsumerOffsets = offsets.Count,
            DeletedConsumers = deletableConsumers.Count
        };
    }

    public async Task<DeleteConsumerGroupResponse> DeleteConsumerGroupAsync(string consumerGroup)
    {
        // Delete consumer offsets for this group
        var offsets = await _context.ConsumerOffsets
            .Where(o => o.ConsumerGroup == consumerGroup)
            .ToListAsync();
        if (offsets.Count > 0)
            _context.ConsumerOffsets.RemoveRange(offsets);

        // Delete consumers in this group
        var consumers = await _context.Consumers
            .Where(c => c.ConsumerGroup == consumerGroup)
            .ToListAsync();
        if (consumers.Count > 0)
            _context.Consumers.RemoveRange(consumers);

        await _context.SaveChangesAsync();

        _logger.LogInformation(
            "Consumer group deleted: {Group}, offsets: {Offsets}, consumers: {Consumers}",
            consumerGroup, offsets.Count, consumers.Count);

        return new DeleteConsumerGroupResponse
        {
            ConsumerGroup = consumerGroup,
            DeletedOffsets = offsets.Count,
            DeletedConsumers = consumers.Count
        };
    }
}
