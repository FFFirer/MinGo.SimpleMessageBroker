using System.Security.Cryptography;
using System.Text;

namespace SimpleMessageBroker.Server.Services;

/// <summary>
/// Strategy interface for custom partition calculation.
/// </summary>
public interface IPartitionStrategy
{
    int GetPartition(string topic, string? key, int partitionCount);
}

public interface IPartitionRouter
{
    int GetPartition(string topic, string? key, int partitionCount);
}

/// <summary>
/// Default partition router using SHA256 hash modulo.
/// </summary>
public class PartitionRouter : IPartitionRouter
{
    private int _roundRobinCounter;

    public int GetPartition(string topic, string? key, int partitionCount)
    {
        if (partitionCount <= 0)
            return 0;

        if (string.IsNullOrEmpty(key))
        {
            // Round-robin when no key is provided
            return Interlocked.Increment(ref _roundRobinCounter) % partitionCount;
        }

        var hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(key));
        var hashValue = BitConverter.ToInt64(hashBytes, 0);
        return (int)(Math.Abs(hashValue) % partitionCount);
    }
}
