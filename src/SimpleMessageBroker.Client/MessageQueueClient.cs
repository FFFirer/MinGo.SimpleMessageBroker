using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using SimpleMessageBroker.Client.Models;
using SimpleMessageBroker.Client.Retry;

namespace SimpleMessageBroker.Client;

/// <summary>
/// HTTP-based implementation of IMessageQueueClient.
/// </summary>
public class MessageQueueClient : IMessageQueueClient, IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly ExponentialBackoffPolicy _retryPolicy;
    private readonly MessageQueueClientOptions _options;
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNameCaseInsensitive = true
    };

    public MessageQueueClient(HttpClient httpClient, MessageQueueClientOptions options)
    {
        _httpClient = httpClient;
        _options = options;
        _retryPolicy = new ExponentialBackoffPolicy(
            options.MaxRetries,
            options.InitialBackoffMs,
            options.MaxBackoffMs,
            options.BackoffMultiplier);

        _httpClient.BaseAddress = new Uri(options.BaseAddress);
        _httpClient.Timeout = options.DefaultTimeout;

        if (!string.IsNullOrEmpty(options.ApiKey))
        {
            _httpClient.DefaultRequestHeaders.Add("X-Api-Key", options.ApiKey);
        }
    }

    public async Task<ProduceResult> ProduceAsync(
        string topic,
        byte[] payload,
        string? key = null,
        string? contentType = null,
        Dictionary<string, string>? headers = null,
        int? ttlSeconds = null,
        CancellationToken cancellationToken = default)
    {
        var request = new
        {
            topic,
            key,
            payload = Convert.ToBase64String(payload),
            contentType = contentType ?? "application/octet-stream",
            headers,
            ttlSeconds
        };

        return await _retryPolicy.ExecuteAsync(async () =>
        {
            var response = await _httpClient.PostAsJsonAsync("api/v1/producer/messages", request, JsonOptions, cancellationToken);
            response.EnsureSuccessStatusCode();
            var apiResponse = await response.Content.ReadFromJsonAsync<ApiResponse<ProduceResult>>(JsonOptions, cancellationToken);
            return apiResponse!.Data!;
        }, cancellationToken);
    }

    public async Task<IReadOnlyList<ProduceResult>> ProduceBatchAsync(
        IReadOnlyList<ProduceMessage> messages,
        CancellationToken cancellationToken = default)
    {
        var requestMessages = messages.Select(m => new
        {
            topic = m.Topic,
            key = m.Key,
            payload = Convert.ToBase64String(m.Payload),
            contentType = m.ContentType ?? "application/octet-stream",
            headers = m.Headers,
            ttlSeconds = m.TtlSeconds
        }).ToList();

        var request = new { messages = requestMessages };

        return await _retryPolicy.ExecuteAsync(async () =>
        {
            var response = await _httpClient.PostAsJsonAsync("api/v1/producer/messages/batch", request, JsonOptions, cancellationToken);
            response.EnsureSuccessStatusCode();
            var apiResponse = await response.Content.ReadFromJsonAsync<ApiResponse<BatchProduceResult>>(JsonOptions, cancellationToken);
            return (IReadOnlyList<ProduceResult>)apiResponse!.Data!.Results;
        }, cancellationToken);
    }

    public async Task<ConsumeResult> ConsumeAsync(
        string topic,
        string consumerGroup,
        string? consumerId = null,
        int batchSize = 10,
        int timeoutSeconds = 30,
        CancellationToken cancellationToken = default)
    {
        var request = new
        {
            topic,
            consumerGroup,
            consumerId,
            batchSize,
            timeoutSeconds
        };

        return await _retryPolicy.ExecuteAsync(async () =>
        {
            var response = await _httpClient.PostAsJsonAsync("api/v1/consumer/pull", request, JsonOptions, cancellationToken);
            response.EnsureSuccessStatusCode();
            var content = await response.Content.ReadAsStringAsync(cancellationToken);
            var apiResponse = JsonSerializer.Deserialize<ApiResponse<ConsumeResultDto>>(content, JsonOptions);
            var data = apiResponse!.Data!;

            // Convert base64 payloads to byte[]
            var result = new ConsumeResult
            {
                Count = data.Count,
                HasMore = data.HasMore,
                Messages = data.Messages.Select(m => new MqMessage
                {
                    Id = m.Id,
                    Topic = m.Topic,
                    Key = m.Key,
                    Partition = m.Partition,
                    Payload = Convert.FromBase64String(m.PayloadBase64),
                    ContentType = m.ContentType,
                    Headers = m.Headers,
                    CreatedAt = m.CreatedAt
                }).ToList()
            };
            return result;
        }, cancellationToken);
    }

    public async Task<bool> AcknowledgeAsync(
        string messageId,
        string consumerGroup,
        string? consumerId = null,
        CancellationToken cancellationToken = default)
    {
        var url = $"api/v1/consumer/ack/{messageId}?consumerGroup={Uri.EscapeDataString(consumerGroup)}";
        if (consumerId != null)
            url += $"&consumerId={Uri.EscapeDataString(consumerId)}";

        return await _retryPolicy.ExecuteAsync(async () =>
        {
            var response = await _httpClient.PostAsync(url, null, cancellationToken);
            response.EnsureSuccessStatusCode();
            var apiResponse = await response.Content.ReadFromJsonAsync<ApiResponse<bool>>(JsonOptions, cancellationToken);
            return apiResponse!.Data;
        }, cancellationToken);
    }

    public async Task<BatchAckResult> AcknowledgeBatchAsync(
        IReadOnlyList<string> messageIds,
        string consumerGroup,
        string? consumerId = null,
        CancellationToken cancellationToken = default)
    {
        var request = new
        {
            messageIds,
            consumerGroup,
            consumerId
        };

        return await _retryPolicy.ExecuteAsync(async () =>
        {
            var response = await _httpClient.PostAsJsonAsync("api/v1/consumer/ack/batch", request, JsonOptions, cancellationToken);
            response.EnsureSuccessStatusCode();
            var apiResponse = await response.Content.ReadFromJsonAsync<ApiResponse<BatchAckResult>>(JsonOptions, cancellationToken);
            return apiResponse!.Data!;
        }, cancellationToken);
    }

    public async Task<long> GetQueueDepthAsync(
        string topic,
        string? consumerGroup = null,
        CancellationToken cancellationToken = default)
    {
        var url = $"api/v1/admin/topics/{Uri.EscapeDataString(topic)}/depth";
        if (consumerGroup != null)
            url += $"?consumerGroup={Uri.EscapeDataString(consumerGroup)}";

        return await _retryPolicy.ExecuteAsync(async () =>
        {
            var response = await _httpClient.GetAsync(url, cancellationToken);
            response.EnsureSuccessStatusCode();
            var apiResponse = await response.Content.ReadFromJsonAsync<ApiResponse<QueueDepthDto>>(JsonOptions, cancellationToken);
            return apiResponse!.Data!.Depth;
        }, cancellationToken);
    }

    public void Dispose()
    {
        // HttpClient is managed by IHttpClientFactory, no need to dispose here
        GC.SuppressFinalize(this);
    }

    // Internal DTOs for JSON deserialization of base64 payloads
    private class ApiResponse<T>
    {
        public bool Success { get; set; }
        public string Message { get; set; } = string.Empty;
        public T? Data { get; set; }
        public string? ErrorCode { get; set; }
    }

    private class BatchProduceResult
    {
        public List<ProduceResult> Results { get; set; } = new();
        public int TotalCount { get; set; }
    }

    private class ConsumeResultDto
    {
        public List<ConsumedMessageDto> Messages { get; set; } = new();
        public int Count { get; set; }
        public bool HasMore { get; set; }
    }

    private class ConsumedMessageDto
    {
        public string Id { get; set; } = string.Empty;
        public string Topic { get; set; } = string.Empty;
        public string? Key { get; set; }
        public int Partition { get; set; }
        public string PayloadBase64 { get; set; } = string.Empty;
        public string ContentType { get; set; } = string.Empty;
        public Dictionary<string, string>? Headers { get; set; }
        public DateTime CreatedAt { get; set; }
    }

    private class QueueDepthDto
    {
        public string Topic { get; set; } = string.Empty;
        public string? ConsumerGroup { get; set; }
        public long Depth { get; set; }
    }
}
