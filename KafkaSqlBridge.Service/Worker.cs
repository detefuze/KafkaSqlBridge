using KafkaSqlBridge.Core.Interfaces;

namespace KafkaSqlBridge.Service;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IKafkaConsumerService _kafkaConsumerService;

    public Worker(
        ILogger<Worker> logger,
        IKafkaConsumerService kafkaConsumerService)
    {
        _logger = logger;
        _kafkaConsumerService = kafkaConsumerService;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker service starting at: {Time}", DateTimeOffset.Now);

        try
        {
            // Запуск Kafka Consumer
            await _kafkaConsumerService.StartConsumingAsync(stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in worker service");
            throw;
        }

        _logger.LogInformation("Worker service stopping at: {Time}", DateTimeOffset.Now);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping worker service gracefully...");

        _kafkaConsumerService.StopConsuming();

        await base.StopAsync(cancellationToken);
    }
}