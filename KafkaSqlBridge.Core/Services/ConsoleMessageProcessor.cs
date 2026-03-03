using KafkaSqlBridge.Core.Models;
using Microsoft.Extensions.Logging;

namespace KafkaSqlBridge.Core.Services;

// Вывод сообщения в консоль, обработчик сообщения
public class ConsoleMessageProcessor : IMessageProcessor
{

    private readonly ILogger<ConsoleMessageProcessor> _logger;

    public ConsoleMessageProcessor(ILogger<ConsoleMessageProcessor> logger)
    {
        _logger = logger;
    }

    public async Task ProcessMessageAsync(ErpMessage message, CancellationToken cancellationToken)
    {
        // Имитация обработки
        await Task.Delay(100, cancellationToken);

        Console.WriteLine("=== Обработка сообщения ===");
        Console.WriteLine($"ID: {message.MessageId}");
        Console.WriteLine($"Операция: {message.OperationType}");
        Console.WriteLine($"Сущность: {message.EntityType}:{message.EntityId}");
        Console.WriteLine($"Время: {message.Timestamp:HH:mm:ss}");
        Console.WriteLine($"Данные: {message.Payload}");
        Console.WriteLine("===========================\n");

        _logger.LogInformation("Message processed: {MessageId}", message.MessageId);
    }
}
