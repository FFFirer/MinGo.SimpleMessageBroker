using Microsoft.AspNetCore.Mvc;
using SimpleMessageBroker.Server.DTOs;
using SimpleMessageBroker.Server.Services;

namespace SimpleMessageBroker.Server.Controllers;

[ApiController]
[Route("api/v1/producer")]
public class ProducerController : ControllerBase
{
    private readonly IMessageService _messageService;
    private readonly ILogger<ProducerController> _logger;

    public ProducerController(IMessageService messageService, ILogger<ProducerController> logger)
    {
        _messageService = messageService;
        _logger = logger;
    }

    [HttpPost("messages")]
    public async Task<ActionResult<ApiResponse<ProduceResponse>>> Produce([FromBody] ProduceRequest request)
    {
        try
        {
            var result = await _messageService.ProduceAsync(request);
            return Ok(ApiResponse<ProduceResponse>.Ok(result, "Message produced successfully"));
        }
        catch (InvalidOperationException ex)
        {
            return NotFound(ApiResponse<ProduceResponse>.Fail(ex.Message, ErrorCodes.TopicNotFound));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error producing message to topic {Topic}", request.Topic);
            return StatusCode(500, ApiResponse<ProduceResponse>.Fail("Internal server error", ErrorCodes.InternalError));
        }
    }

    [HttpPost("messages/batch")]
    public async Task<ActionResult<ApiResponse<BatchProduceResponse>>> ProduceBatch([FromBody] BatchProduceRequest request)
    {
        try
        {
            var result = await _messageService.ProduceBatchAsync(request.Messages);
            return Ok(ApiResponse<BatchProduceResponse>.Ok(result, "Batch messages produced successfully"));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in batch produce");
            return StatusCode(500, ApiResponse<BatchProduceResponse>.Fail("Internal server error", ErrorCodes.InternalError));
        }
    }
}
