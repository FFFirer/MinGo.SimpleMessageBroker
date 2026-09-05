using SimpleMessageBroker.Client.Models;

namespace SimpleMessageBroker.Client;

/// <summary>
/// Generic message handler for typed consumption.
/// </summary>
public interface IMessageHandler<T>
{
    Task HandleAsync(T message, MessageContext context, CancellationToken cancellationToken = default);
}
