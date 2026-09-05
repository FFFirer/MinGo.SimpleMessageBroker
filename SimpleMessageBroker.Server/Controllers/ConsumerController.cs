using Microsoft.AspNetCore.Mvc;
using SimpleMessageBroker.Server.DTOs;
using SimpleMessageBroker.Server.Services;

namespace SimpleMessageBroker.Server.Controllers;

[ApiController]
[Route("api/v1/consumer")]
public class ConsumerController : ControllerBase
{
    private readonly IMessageService _messageService;
    private readonly ILogger<ConsumerController> _logger;

    public ConsumerController(IMessageService messageService, ILogger<ConsumerController> logger)
    {
        _messageService = messageService;
        _logger = logger;
    }

    [HttpPost("pull")]
    public async Task<ActionResult<ApiResponse<ConsumeResponse>>> Consume([FromBody] ConsumeRequest request)
    {
        try
        {
            var result = await _messageService.ConsumeAsync(request);
            return Ok(ApiResponse<ConsumeResponse>.Ok(result, "Messages consumed successfully"));
        }
        catch (InvalidOperationException ex)
        {
            return NotFound(ApiResponse<ConsumeResponse>.Fail(ex.Message, ErrorCodes.TopicNotFound));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error consuming messages from topic {Topic}", request.Topic);
            return StatusCode(500, ApiResponse<ConsumeResponse>.Fail("Internal server error", ErrorCodes.InternalError));
        }
    }

    [HttpPost("ack/{messageId}")]
    public async Task<ActionResult<ApiResponse<bool>>> Acknowledge(
        string messageId,
        [FromQuery] string consumerGroup,
        [FromQuery] string? consumerId)
    {
        try
        {
            var result = await _messageService.AckMessageAsync(messageId, consumerGroup, consumerId);
            if (result)
                return Ok(ApiResponse<bool>.Ok(true, "Message acknowledged"));

            return NotFound(ApiResponse<bool>.Fail("Message not found or group mismatch", ErrorCodes.MessageNotFound));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error acknowledging message {MessageId}", messageId);
            return StatusCode(500, ApiResponse<bool>.Fail("Internal server error", ErrorCodes.InternalError));
        }
    }

    [HttpPost("ack/batch")]
    public async Task<ActionResult<ApiResponse<BatchAckResponse>>> AckBatch([FromBody] BatchAckRequest request)
    {
        try
        {
            var result = await _messageService.AckBatchAsync(request.MessageIds, request.ConsumerGroup, request.ConsumerId);
            return Ok(ApiResponse<BatchAckResponse>.Ok(result, "Messages acknowledged"));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in batch acknowledge");
            return StatusCode(500, ApiResponse<BatchAckResponse>.Fail("Internal server error", ErrorCodes.InternalError));
        }
    }
}
