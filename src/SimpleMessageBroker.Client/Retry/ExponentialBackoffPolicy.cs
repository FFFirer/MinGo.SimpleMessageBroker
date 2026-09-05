namespace SimpleMessageBroker.Client.Retry;

/// <summary>
/// Exponential backoff retry policy.
/// Retries on HTTP 5xx, network timeout, and connection failures.
/// Does NOT retry on 4xx errors.
/// </summary>
public class ExponentialBackoffPolicy
{
    private readonly int _maxRetries;
    private readonly int _initialBackoffMs;
    private readonly int _maxBackoffMs;
    private readonly double _multiplier;

    public ExponentialBackoffPolicy(int maxRetries = 3, int initialBackoffMs = 100, int maxBackoffMs = 5000, double multiplier = 2.0)
    {
        _maxRetries = maxRetries;
        _initialBackoffMs = initialBackoffMs;
        _maxBackoffMs = maxBackoffMs;
        _multiplier = multiplier;
    }

    public async Task<T> ExecuteAsync<T>(Func<Task<T>> action, CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        var backoff = _initialBackoffMs;

        while (true)
        {
            try
            {
                return await action().ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < _maxRetries && IsRetriable(ex))
            {
                attempt++;
                await Task.Delay(backoff, cancellationToken).ConfigureAwait(false);
                backoff = Math.Min((int)(backoff * _multiplier), _maxBackoffMs);
            }
        }
    }

    public async Task ExecuteAsync(Func<Task> action, CancellationToken cancellationToken = default)
    {
        await ExecuteAsync(async () =>
        {
            await action().ConfigureAwait(false);
            return true;
        }, cancellationToken).ConfigureAwait(false);
    }

    private static bool IsRetriable(Exception ex)
    {
        // Retry on HttpRequestException (network errors, 5xx)
        if (ex is HttpRequestException httpEx)
        {
            return httpEx.StatusCode == null || (int)httpEx.StatusCode >= 500;
        }

        // Retry on timeout
        if (ex is TaskCanceledException or TimeoutException)
            return true;

        return false;
    }
}
