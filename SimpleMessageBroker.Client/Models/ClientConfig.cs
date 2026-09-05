namespace SimpleMessageBroker.Client.Models;

/// <summary>
/// Client configuration options.
/// </summary>
public class MessageQueueClientOptions
{
    public string BaseAddress { get; set; } = "http://localhost:5000";
    public string? ApiKey { get; set; }
    public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public int MaxRetries { get; set; } = 3;
    public int InitialBackoffMs { get; set; } = 100;
    public int MaxBackoffMs { get; set; } = 5000;
    public double BackoffMultiplier { get; set; } = 2.0;
    public int MaxConnectionsPerServer { get; set; } = 50;
}
