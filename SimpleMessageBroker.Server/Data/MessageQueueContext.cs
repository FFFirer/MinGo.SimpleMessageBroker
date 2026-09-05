using Microsoft.EntityFrameworkCore;
using SimpleMessageBroker.Server.Models;

namespace SimpleMessageBroker.Server.Data;

public class MessageQueueContext : DbContext
{
    public MessageQueueContext(DbContextOptions<MessageQueueContext> options)
        : base(options)
    {
    }

    public DbSet<Message> Messages => Set<Message>();
    public DbSet<Topic> Topics => Set<Topic>();
    public DbSet<ConsumerOffset> ConsumerOffsets => Set<ConsumerOffset>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Message>(entity =>
        {
            entity.Property(m => m.Payload).IsRequired();
            entity.Property(m => m.ContentType).HasDefaultValue("application/octet-stream");
            entity.Property(m => m.IsConsumed).HasDefaultValue(false);
            entity.Property(m => m.RetryCount).HasDefaultValue(0);
        });

        modelBuilder.Entity<Topic>(entity =>
        {
            entity.Property(t => t.PartitionCount).HasDefaultValue(10);
            entity.Property(t => t.DefaultTtlSeconds).HasDefaultValue(86400);
            entity.Property(t => t.IsActive).HasDefaultValue(true);
        });

        modelBuilder.Entity<ConsumerOffset>(entity =>
        {
            entity.Property(c => c.LastOffset).HasDefaultValue(0L);
        });
    }
}
