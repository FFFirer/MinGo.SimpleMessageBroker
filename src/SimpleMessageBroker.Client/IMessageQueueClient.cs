using SimpleMessageBroker.Client.Models;

namespace SimpleMessageBroker.Client;

/// <summary>
/// Core client interface for the message queue. All operations use byte[] payload.
/// </summary>
public interface IMessageQueueClient
{
    /// <summary>
    /// Produce a single message.
    /// </summary>
    Task<ProduceResult> ProduceAsync(
        string topic,
        byte[] payload,
        string? key = null,
        string? contentType = null,
        Dictionary<string, string>? headers = null,
        int? ttlSeconds = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Produce a batch of messages.
    /// </summary>
    Task<IReadOnlyList<ProduceResult>> ProduceBatchAsync(
        IReadOnlyList<ProduceMessage> messages,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Pull messages (returns raw byte[] payloads).
    /// </summary>
    Task<ConsumeResult> ConsumeAsync(
        string topic,
        string consumerGroup,
        string? consumerId = null,
        int batchSize = 10,
        int timeoutSeconds = 30,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledge a single message.
    /// </summary>
    Task<bool> AcknowledgeAsync(
        string messageId,
        string consumerGroup,
        string? consumerId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledge a batch of messages.
    /// </summary>
    Task<BatchAckResult> AcknowledgeBatchAsync(
        IReadOnlyList<string> messageIds,
        string consumerGroup,
        string? consumerId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get queue depth for a topic.
    /// </summary>
    Task<long> GetQueueDepthAsync(
        string topic,
        string? consumerGroup = null,
        CancellationToken cancellationToken = default);
}
