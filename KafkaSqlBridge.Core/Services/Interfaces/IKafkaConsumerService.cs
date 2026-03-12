namespace KafkaSqlBridge.Core.Services.Interfaces;

/// Интерфейс для Kafka Consumer
public interface IKafkaConsumerService
{
    Task StartConsumingAsync(CancellationToken cancellationToken);

    void StopConsuming();
}