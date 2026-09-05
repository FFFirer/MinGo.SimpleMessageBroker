using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace SimpleMessageBroker.Server.Models;

[Table("ConsumerOffsets")]
[Index(nameof(Topic), nameof(ConsumerGroup), nameof(Partition), Name = "IX_ConsumerOffsets_Unique", IsUnique = true)]
public class ConsumerOffset
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public long Id { get; set; }

    [Required]
    [MaxLength(100)]
    public string Topic { get; set; } = string.Empty;

    [Required]
    [MaxLength(100)]
    public string ConsumerGroup { get; set; } = string.Empty;

    public int Partition { get; set; }

    public long LastOffset { get; set; }

    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;

    // Navigation
    [ForeignKey(nameof(Topic))]
    public Topic? TopicEntity { get; set; }
}
