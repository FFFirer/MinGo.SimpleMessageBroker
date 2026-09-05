using Microsoft.VisualStudio.TestTools.UnitTesting;
using SimpleMessageBroker.Client.Models;

namespace SimpleMessageBroker.Client.Tests;

[TestClass]
public class MessageModelsTests
{
    [TestMethod]
    public void MqMessage_DefaultPayload_IsEmpty()
    {
        var msg = new MqMessage();
        Assert.AreEqual(0, msg.Payload.Length);
    }

    [TestMethod]
    public void ProduceResult_Properties_SetCorrectly()
    {
        var result = new ProduceResult
        {
            MessageId = "msg-001",
            Partition = 2,
            CreatedAt = DateTime.UtcNow
        };

        Assert.AreEqual("msg-001", result.MessageId);
        Assert.AreEqual(2, result.Partition);
    }

    [TestMethod]
    public void ConsumeResult_EmptyByDefault()
    {
        var result = new ConsumeResult();
        Assert.AreEqual(0, result.Count);
        Assert.IsFalse(result.HasMore);
        Assert.IsNotNull(result.Messages);
    }

    [TestMethod]
    public void BatchAckResult_SumMatchesTotal()
    {
        var result = new BatchAckResult { Acknowledged = 8, Failed = 2 };
        Assert.AreEqual(10, result.Acknowledged + result.Failed);
    }
}
