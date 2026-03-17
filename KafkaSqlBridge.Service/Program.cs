using KafkaSqlBridge.Core.Configuration;
using KafkaSqlBridge.Core.Handlers;
using KafkaSqlBridge.Core.Interfaces;
using KafkaSqlBridge.Core.Services;
using KafkaSqlBridge.Service;
using Microsoft.Extensions.Options;

// Создаем builder приложения (аналог Spring Boot Application)
var builder = Host.CreateApplicationBuilder(args);

// Регистрируем конфигурацию appsettings.json
builder.Configuration.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

// Биндим настройки на класс KafkaSettings 
builder.Services.Configure<KafkaSettings>(
    builder.Configuration.GetSection("KafkaSettings"));

// Передаем connectionString
var pmsSettings = builder.Configuration.GetSection("PMSDatabaseSettings").Get<PMSDatabaseSettings>();

var connectionString = pmsSettings?.ConnectionString;

// Регистрируем сервисы и WorkerService
builder.Services.AddSingleton<IMessageHandler, ProductMessageHandler>();
builder.Services.AddSingleton<IMessageHandler, MaterialMessageHandler>();
builder.Services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();
builder.Services.AddSingleton<IDatabaseService>(ds => new DatabaseService(connectionString));
builder.Services.AddHostedService<Worker>();

// Собираем и запускаем хост
var host = builder.Build();

// Выводим информацию о конфигурации
var kafkaSettings = host.Services.GetRequiredService<IOptions<KafkaSettings>>().Value;
Console.WriteLine("=== Kafka SQL Bridge Service ===");
Console.WriteLine($"Kafka: {kafkaSettings.BootstrapServers}");
Console.WriteLine("================================\n");

await host.RunAsync();