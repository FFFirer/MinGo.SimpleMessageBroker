using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace SimpleMessageBroker.Server.Models;

[Table("Messages")]
[Index(nameof(Topic), nameof(Partition), nameof(CreatedAt), Name = "IX_Messages_Topic_Partition")]
[Index(nameof(ExpiresAt), Name = "IX_Messages_ExpiresAt")]
[Index(nameof(Topic), nameof(ConsumerGroup), nameof(IsConsumed), Name = "IX_Messages_Consumer")]
public class Message
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    [MaxLength(36)]
    public string Id { get; set; } = Guid.NewGuid().ToString();

    [Required]
    [MaxLength(100)]
    public string Topic { get; set; } = string.Empty;

    [MaxLength(200)]
    public string? Key { get; set; }

    public int Partition { get; set; }

    [Required]
    public byte[] Payload { get; set; } = Array.Empty<byte>();

    [MaxLength(100)]
    public string ContentType { get; set; } = "application/octet-stream";

    /// <summary>
    /// JSON-serialized dictionary for message headers.
    /// </summary>
    public string? Headers { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    public DateTime? ExpiresAt { get; set; }

    public int RetryCount { get; set; }

    public bool IsConsumed { get; set; }

    [MaxLength(100)]
    public string? ConsumerGroup { get; set; }

    [MaxLength(200)]
    public string? ConsumerId { get; set; }

    public DateTime? ConsumedAt { get; set; }

    // Navigation
    [ForeignKey(nameof(Topic))]
    public Topic? TopicEntity { get; set; }
}
