using SimpleMessageBroker.Server.Configuration;

namespace SimpleMessageBroker.Server.Services;

public class CleanupService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<CleanupService> _logger;
    private readonly MessageQueueOptions _options;

    public CleanupService(
        IServiceProvider serviceProvider,
        ILogger<CleanupService> logger,
        MessageQueueOptions options)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _options = options;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("CleanupService started. Interval: {Interval} minutes", _options.CleanupIntervalMinutes);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromMinutes(_options.CleanupIntervalMinutes), stoppingToken);

                using var scope = _serviceProvider.CreateScope();
                var messageService = scope.ServiceProvider.GetRequiredService<IMessageService>();

                _logger.LogInformation("CleanupService running cleanup cycle...");
                var result = await messageService.CleanupAsync();

                _logger.LogInformation(
                    "CleanupService cycle completed. Deleted: {Total} (expired: {Expired}, consumed: {Consumed})",
                    result.TotalDeleted, result.DeletedExpired, result.DeletedConsumed);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in CleanupService cycle");
                // Wait 1 minute before retrying on error
                await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
            }
        }

        _logger.LogInformation("CleanupService stopped");
    }
}

