using Microsoft.EntityFrameworkCore;
using SimpleMessageBroker.Server.Configuration;
using SimpleMessageBroker.Server.Data;
using SimpleMessageBroker.Server.Middleware;
using SimpleMessageBroker.Server.Services;

var builder = WebApplication.CreateBuilder(args);

// Configuration binding
builder.Services.Configure<MessageQueueOptions>(
    builder.Configuration.GetSection(MessageQueueOptions.SectionName));
builder.Services.Configure<AuthenticationOptions>(
    builder.Configuration.GetSection(AuthenticationOptions.SectionName));
builder.Services.Configure<CorsOptions>(
    builder.Configuration.GetSection(CorsOptions.SectionName));

// Build options for DI registration
var mqOptions = new MessageQueueOptions();
builder.Configuration.GetSection(MessageQueueOptions.SectionName).Bind(mqOptions);
builder.Services.AddSingleton(mqOptions);

// Database
var dbProvider = builder.Configuration["DatabaseProvider"] ?? "Sqlite";
builder.Services.AddDbContext<MessageQueueContext>(options =>
{
    if (dbProvider.Equals("Postgres", StringComparison.OrdinalIgnoreCase))
    {
        options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection"));
    }
    else
    {
        options.UseSqlite(builder.Configuration.GetConnectionString("DefaultConnection"));
    }
});

// Services
builder.Services.AddScoped<IPartitionRouter, PartitionRouter>();
builder.Services.AddScoped<IMessageService, MessageService>();
builder.Services.AddHostedService<CleanupService>();

// Controllers
builder.Services.AddControllers()
    .AddJsonOptions(options =>
    {
        options.JsonSerializerOptions.PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase;
    });

// Health checks
builder.Services.AddHealthChecks()
    .AddDbContextCheck<MessageQueueContext>("database");

// CORS
var corsConfig = new CorsOptions();
builder.Configuration.GetSection(CorsOptions.SectionName).Bind(corsConfig);
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.WithOrigins(corsConfig.AllowedOrigins)
              .WithMethods(corsConfig.AllowedMethods)
              .WithHeaders(corsConfig.AllowedHeaders);
    });
});

// Swagger
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Ensure database is created
using (var scope = app.Services.CreateScope())
{
    var dbContext = scope.ServiceProvider.GetRequiredService<MessageQueueContext>();
    await dbContext.Database.EnsureCreatedAsync();
}

// Middleware pipeline
app.UseMiddleware<GlobalExceptionMiddleware>();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseCors();
app.UseAuthorization();
app.MapControllers();
app.MapHealthChecks("/health");

app.Run();
