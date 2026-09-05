namespace SimpleMessageBroker.Client;

/// <summary>
/// Serialization aspect interface — user provides implementation.
/// SDK does NOT include any built-in serializer.
/// </summary>
public interface IPayloadSerializer
{
    /// <summary>
    /// Serialize an object to byte[].
    /// </summary>
    byte[] Serialize<T>(T obj);

    /// <summary>
    /// Deserialize byte[] to an object.
    /// </summary>
    T Deserialize<T>(byte[] data);
}
