using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace SimpleMessageBroker.Server.Models;

[Table("Topics")]
public class Topic
{
    [Key]
    [MaxLength(100)]
    public string Name { get; set; } = string.Empty;

    public int PartitionCount { get; set; } = 10;

    public int DefaultTtlSeconds { get; set; } = 86400;

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    public bool IsActive { get; set; } = true;
}
