using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SimpleMessageBroker.Server.Data.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Topics",
                columns: table => new
                {
                    Name = table.Column<string>(type: "TEXT", maxLength: 100, nullable: false),
                    PartitionCount = table.Column<int>(type: "INTEGER", nullable: false, defaultValue: 10),
                    DefaultTtlSeconds = table.Column<int>(type: "INTEGER", nullable: false, defaultValue: 86400),
                    CreatedAt = table.Column<DateTime>(type: "TEXT", nullable: false),
                    IsActive = table.Column<bool>(type: "INTEGER", nullable: false, defaultValue: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Topics", x => x.Name);
                });

            migrationBuilder.CreateTable(
                name: "ConsumerOffsets",
                columns: table => new
                {
                    Id = table.Column<long>(type: "INTEGER", nullable: false)
                        .Annotation("Sqlite:Autoincrement", true),
                    Topic = table.Column<string>(type: "TEXT", maxLength: 100, nullable: false),
                    ConsumerGroup = table.Column<string>(type: "TEXT", maxLength: 100, nullable: false),
                    Partition = table.Column<int>(type: "INTEGER", nullable: false),
                    LastOffset = table.Column<long>(type: "INTEGER", nullable: false, defaultValue: 0L),
                    UpdatedAt = table.Column<DateTime>(type: "TEXT", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ConsumerOffsets", x => x.Id);
                    table.ForeignKey(
                        name: "FK_ConsumerOffsets_Topics_Topic",
                        column: x => x.Topic,
                        principalTable: "Topics",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Messages",
                columns: table => new
                {
                    Id = table.Column<string>(type: "TEXT", maxLength: 36, nullable: false),
                    Topic = table.Column<string>(type: "TEXT", maxLength: 100, nullable: false),
                    Key = table.Column<string>(type: "TEXT", maxLength: 200, nullable: true),
                    Partition = table.Column<int>(type: "INTEGER", nullable: false),
                    Payload = table.Column<byte[]>(type: "BLOB", nullable: false),
                    ContentType = table.Column<string>(type: "TEXT", maxLength: 100, nullable: false, defaultValue: "application/octet-stream"),
                    Headers = table.Column<string>(type: "TEXT", nullable: true),
                    CreatedAt = table.Column<DateTime>(type: "TEXT", nullable: false),
                    ExpiresAt = table.Column<DateTime>(type: "TEXT", nullable: true),
                    RetryCount = table.Column<int>(type: "INTEGER", nullable: false, defaultValue: 0),
                    IsConsumed = table.Column<bool>(type: "INTEGER", nullable: false, defaultValue: false),
                    ConsumerGroup = table.Column<string>(type: "TEXT", maxLength: 100, nullable: true),
                    ConsumerId = table.Column<string>(type: "TEXT", maxLength: 200, nullable: true),
                    ConsumedAt = table.Column<DateTime>(type: "TEXT", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Messages", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Messages_Topics_Topic",
                        column: x => x.Topic,
                        principalTable: "Topics",
                        principalColumn: "Name",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_ConsumerOffsets_Unique",
                table: "ConsumerOffsets",
                columns: new[] { "Topic", "ConsumerGroup", "Partition" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Messages_Consumer",
                table: "Messages",
                columns: new[] { "Topic", "ConsumerGroup", "IsConsumed" });

            migrationBuilder.CreateIndex(
                name: "IX_Messages_ExpiresAt",
                table: "Messages",
                column: "ExpiresAt");

            migrationBuilder.CreateIndex(
                name: "IX_Messages_Topic_Partition",
                table: "Messages",
                columns: new[] { "Topic", "Partition", "CreatedAt" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "ConsumerOffsets");

            migrationBuilder.DropTable(
                name: "Messages");

            migrationBuilder.DropTable(
                name: "Topics");
        }
    }
}
