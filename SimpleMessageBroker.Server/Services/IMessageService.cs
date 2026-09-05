using SimpleMessageBroker.Server.DTOs;

namespace SimpleMessageBroker.Server.Services;

public interface IMessageService
{
    Task<ProduceResponse> ProduceAsync(ProduceRequest request);
    Task<BatchProduceResponse> ProduceBatchAsync(IEnumerable<ProduceRequest> requests);
    Task<ConsumeResponse> ConsumeAsync(ConsumeRequest request);
    Task<bool> AckMessageAsync(string messageId, string consumerGroup, string? consumerId);
    Task<BatchAckResponse> AckBatchAsync(IEnumerable<string> messageIds, string consumerGroup, string? consumerId);
    Task<CleanupResponse> CleanupAsync();
    Task<QueueDepthResponse> GetQueueDepthAsync(string topic, string? consumerGroup);
}
