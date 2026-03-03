using KafkaSqlBridge.Core.Configuration;
using KafkaSqlBridge.Core.Services;
using KafkaSqlBridge.Service;
using Microsoft.Extensions.Options;

// Создаем builder приложения (аналог Spring Boot Application)
var builder = Host.CreateApplicationBuilder(args);

// 1. Регистрируем конфигурацию
// Добавляем appsettings.json
builder.Configuration.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

// Биндим настройки на классы 
builder.Services.Configure<KafkaSettings>(
    builder.Configuration.GetSection("KafkaSettings")); // KafkaSettings из appsettings.json

// 2. Регистрируем сервисы (DI)
builder.Services.AddSingleton<IMessageProcessor, ConsoleMessageProcessor>();
builder.Services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();

// 3. Регистрируем Worker Service
builder.Services.AddHostedService<Worker>();

// 4. Собираем и запускаем хост
var host = builder.Build();

// Выводим информацию о конфигурации
var kafkaSettings = host.Services.GetRequiredService<IOptions<KafkaSettings>>().Value;
Console.WriteLine("=== Kafka SQL Bridge Service ===");
Console.WriteLine($"Kafka: {kafkaSettings.BootstrapServers}");
Console.WriteLine($"Topic: {kafkaSettings.Topic}");
Console.WriteLine($"Group: {kafkaSettings.GroupId}");
Console.WriteLine("================================\n");

await host.RunAsync();