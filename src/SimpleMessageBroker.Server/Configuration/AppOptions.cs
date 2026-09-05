namespace SimpleMessageBroker.Server.Configuration;

public class MessageQueueOptions
{
    public const string SectionName = "MessageQueue";

    public int CleanupIntervalMinutes { get; set; } = 30;
    public int DefaultTtlSeconds { get; set; } = 86400;
    public int RetainConsumedMinutes { get; set; } = 60;
    public int CleanupBatchSize { get; set; } = 1000;
    public int MaxBatchSize { get; set; } = 100;
    public int DefaultPartitionCount { get; set; } = 10;
}

public class AuthenticationOptions
{
    public const string SectionName = "Authentication";

    public bool Enabled { get; set; }
    public List<ApiKeyConfig> ApiKeys { get; set; } = new();
    public bool HmacEnabled { get; set; }
    public string HmacSecret { get; set; } = string.Empty;
}

public class ApiKeyConfig
{
    public string Key { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public bool IsActive { get; set; } = true;
}

public class CorsOptions
{
    public const string SectionName = "Cors";

    public string[] AllowedOrigins { get; set; } = ["*"];
    public string[] AllowedMethods { get; set; } = ["GET", "POST"];
    public string[] AllowedHeaders { get; set; } = ["Content-Type", "X-Api-Key", "X-Request-Id", "X-Client-Id"];
}

public class ApiDocOptions
{
    public const string SectionName = "ApiDoc";

    public bool Enabled { get; set; }
}
