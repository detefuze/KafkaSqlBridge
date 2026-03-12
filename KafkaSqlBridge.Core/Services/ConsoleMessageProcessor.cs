using KafkaSqlBridge.Core.Models;
using KafkaSqlBridge.Core.Services.Interfaces;
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

    public async Task ProcessProductMessageAsync(ProductMessage message, CancellationToken cancellationToken)
    {
        // Имитация обработки
        await Task.Delay(100, cancellationToken);

        Console.WriteLine("=== Обработка сообщения ===");
        Console.WriteLine($"ProductCode: {message.product_code}");
        Console.WriteLine($"ProductName: {message.product_name}");
        Console.WriteLine($"PCS_CA: {message.PCS_CA}");
        Console.WriteLine($"CA_massa_n: {message.CA_massa_n}");
        Console.WriteLine($"CA_massa_b: {message.CA_massa_b}");
        Console.WriteLine($"WipCode: {message.wip_code}");
        Console.WriteLine("===========================\n");

        _logger.LogInformation("Message processed: {MessageId}", message.product_code);
    }
}
