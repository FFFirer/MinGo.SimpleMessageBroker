namespace SimpleMessageBroker.Server.DTOs;

public static class ErrorCodes
{
    public const string Success = "SUCCESS";
    public const string ValidationError = "VALIDATION_ERROR";
    public const string TopicNotFound = "TOPIC_NOT_FOUND";
    public const string MessageNotFound = "MESSAGE_NOT_FOUND";
    public const string MessageExpired = "MESSAGE_EXPIRED";
    public const string MessageAlreadyConsumed = "MESSAGE_ALREADY_CONSUMED";
    public const string ConsumerGroupNotFound = "CONSUMER_GROUP_NOT_FOUND";
    public const string InternalError = "INTERNAL_ERROR";
    public const string DatabaseError = "DATABASE_ERROR";
    public const string RateLimited = "RATE_LIMITED";
}
