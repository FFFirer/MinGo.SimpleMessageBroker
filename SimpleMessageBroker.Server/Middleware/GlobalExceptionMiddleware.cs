using System.Net;
using Microsoft.EntityFrameworkCore;
using SimpleMessageBroker.Server.DTOs;

namespace SimpleMessageBroker.Server.Middleware;

public class GlobalExceptionMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<GlobalExceptionMiddleware> _logger;

    public GlobalExceptionMiddleware(RequestDelegate next, ILogger<GlobalExceptionMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await _next(context);
        }
        catch (DbUpdateException ex)
        {
            _logger.LogError(ex, "Database error occurred");
            await WriteErrorResponseAsync(context, HttpStatusCode.InternalServerError,
                "Database operation failed", ErrorCodes.DatabaseError);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Invalid operation: {Message}", ex.Message);
            await WriteErrorResponseAsync(context, HttpStatusCode.BadRequest,
                ex.Message, ErrorCodes.ValidationError);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled exception occurred. RequestId: {RequestId}", context.TraceIdentifier);
            await WriteErrorResponseAsync(context, HttpStatusCode.InternalServerError,
                "An unexpected error occurred", ErrorCodes.InternalError);
        }
    }

    private static async Task WriteErrorResponseAsync(HttpContext context, HttpStatusCode statusCode, string message, string errorCode)
    {
        context.Response.StatusCode = (int)statusCode;
        context.Response.ContentType = "application/json";

        var response = ApiResponse<object>.Fail(message, errorCode);
        await context.Response.WriteAsJsonAsync(response);
    }
}
