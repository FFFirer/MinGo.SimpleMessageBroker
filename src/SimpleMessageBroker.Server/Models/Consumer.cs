using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace SimpleMessageBroker.Server.Models;

[Table("Consumers")]
[Index(nameof(ConsumerGroup), Name = "IX_Consumers_Group")]
public class Consumer
{
    [Key]
    [MaxLength(200)]
    public string Id { get; set; } = string.Empty;

    [Required]
    [MaxLength(100)]
    public string ConsumerGroup { get; set; } = string.Empty;

    public DateTime LastHeartbeat { get; set; } = DateTime.UtcNow;

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}
