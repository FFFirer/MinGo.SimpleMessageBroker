using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using SimpleMessageBroker.Server.Configuration;
using SimpleMessageBroker.Server.Data;
using SimpleMessageBroker.Server.DTOs;
using SimpleMessageBroker.Server.Models;
using SimpleMessageBroker.Server.Services;

namespace SimpleMessageBroker.Server.Controllers;

[ApiController]
[Route("api/v1/admin")]
public class AdminController : ControllerBase
{
    private readonly IMessageService _messageService;
    private readonly MessageQueueContext _context;
    private readonly MessageQueueOptions _options;
    private readonly ILogger<AdminController> _logger;

    public AdminController(
        IMessageService messageService,
        MessageQueueContext context,
        MessageQueueOptions options,
        ILogger<AdminController> logger)
    {
        _messageService = messageService;
        _context = context;
        _options = options;
        _logger = logger;
    }

    [HttpPost("topics")]
    public async Task<ActionResult<ApiResponse<TopicResponse>>> CreateTopic([FromBody] CreateTopicRequest request)
    {
        try
        {
            var existing = await _context.Topics.FirstOrDefaultAsync(t => t.Name == request.Name);
            if (existing != null)
                return Conflict(ApiResponse<TopicResponse>.Fail($"Topic '{request.Name}' already exists", ErrorCodes.ValidationError));

            var topic = new Topic
            {
                Name = request.Name,
                PartitionCount = request.PartitionCount,
                DefaultTtlSeconds = request.DefaultTtlSeconds,
                CreatedAt = DateTime.UtcNow,
                IsActive = true
            };

            _context.Topics.Add(topic);
            await _context.SaveChangesAsync();

            _logger.LogInformation("Topic created: {Topic}, Partitions: {Partitions}", topic.Name, topic.PartitionCount);

            var response = new TopicResponse
            {
                Name = topic.Name,
                PartitionCount = topic.PartitionCount,
                DefaultTtlSeconds = topic.DefaultTtlSeconds,
                CreatedAt = topic.CreatedAt
            };

            return CreatedAtAction(nameof(CreateTopic), new { topicName = topic.Name },
                ApiResponse<TopicResponse>.Ok(response, "Topic created successfully"));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating topic {Topic}", request.Name);
            return StatusCode(500, ApiResponse<TopicResponse>.Fail("Internal server error", ErrorCodes.InternalError));
        }
    }

    [HttpGet("topics/{topic}/depth")]
    public async Task<ActionResult<ApiResponse<QueueDepthResponse>>> GetQueueDepth(
        string topic,
        [FromQuery] string? consumerGroup)
    {
        try
        {
            var result = await _messageService.GetQueueDepthAsync(topic, consumerGroup);
            return Ok(ApiResponse<QueueDepthResponse>.Ok(result, "Queue depth retrieved"));
        }
        catch (InvalidOperationException ex)
        {
            return NotFound(ApiResponse<QueueDepthResponse>.Fail(ex.Message, ErrorCodes.TopicNotFound));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting queue depth for topic {Topic}", topic);
            return StatusCode(500, ApiResponse<QueueDepthResponse>.Fail("Internal server error", ErrorCodes.InternalError));
        }
    }

    [HttpPost("cleanup")]
    public async Task<ActionResult<ApiResponse<CleanupResponse>>> Cleanup()
    {
        try
        {
            var result = await _messageService.CleanupAsync();
            return Ok(ApiResponse<CleanupResponse>.Ok(result, "Cleanup completed"));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during manual cleanup");
            return StatusCode(500, ApiResponse<CleanupResponse>.Fail("Internal server error", ErrorCodes.InternalError));
        }
    }

    [HttpDelete("topics/{topic}")]
    public async Task<ActionResult<ApiResponse<DeleteTopicResponse>>> DeleteTopic(string topic)
    {
        try
        {
            var result = await _messageService.DeleteTopicAsync(topic);
            _logger.LogInformation("Topic deleted via API: {Topic}", topic);
            return Ok(ApiResponse<DeleteTopicResponse>.Ok(result, $"Topic '{topic}' deleted successfully"));
        }
        catch (InvalidOperationException ex)
        {
            return NotFound(ApiResponse<DeleteTopicResponse>.Fail(ex.Message, ErrorCodes.TopicNotFound));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting topic {Topic}", topic);
            return StatusCode(500, ApiResponse<DeleteTopicResponse>.Fail("Internal server error", ErrorCodes.InternalError));
        }
    }

    [HttpDelete("consumer-groups/{consumerGroup}")]
    public async Task<ActionResult<ApiResponse<DeleteConsumerGroupResponse>>> DeleteConsumerGroup(string consumerGroup)
    {
        try
        {
            var result = await _messageService.DeleteConsumerGroupAsync(consumerGroup);
            _logger.LogInformation("Consumer group deleted via API: {Group}", consumerGroup);
            return Ok(ApiResponse<DeleteConsumerGroupResponse>.Ok(result, $"Consumer group '{consumerGroup}' deleted successfully"));
        }
        catch (InvalidOperationException ex)
        {
            return NotFound(ApiResponse<DeleteConsumerGroupResponse>.Fail(ex.Message, ErrorCodes.ConsumerGroupNotFound));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting consumer group {Group}", consumerGroup);
            return StatusCode(500, ApiResponse<DeleteConsumerGroupResponse>.Fail("Internal server error", ErrorCodes.InternalError));
        }
    }
}
