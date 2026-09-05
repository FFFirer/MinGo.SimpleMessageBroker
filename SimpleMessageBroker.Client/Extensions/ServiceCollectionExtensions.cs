using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using SimpleMessageBroker.Client.Models;

namespace SimpleMessageBroker.Client.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Register SimpleMessageBroker Client SDK with DI.
    /// </summary>
    public static IServiceCollection AddMessageQueueClient(
        this IServiceCollection services,
        Action<MessageQueueClientOptions> configure)
    {
        var options = new MessageQueueClientOptions();
        configure(options);

        services.AddSingleton(options);

        services.AddHttpClient("SimpleMessageBroker", client =>
        {
            client.BaseAddress = new Uri(options.BaseAddress);
            client.Timeout = options.DefaultTimeout;

            if (!string.IsNullOrEmpty(options.ApiKey))
            {
                client.DefaultRequestHeaders.Add("X-Api-Key", options.ApiKey);
            }
        }).ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
        {
            MaxConnectionsPerServer = options.MaxConnectionsPerServer,
            EnableMultipleHttp2Connections = true,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5)
        });

        services.AddSingleton<IMessageQueueClient>(sp =>
        {
            var factory = sp.GetRequiredService<IHttpClientFactory>();
            var httpClient = factory.CreateClient("SimpleMessageBroker");
            return new MessageQueueClient(httpClient, options);
        });

        return services;
    }
}
