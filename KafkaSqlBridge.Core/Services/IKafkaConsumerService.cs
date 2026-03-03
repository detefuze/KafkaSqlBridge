namespace KafkaSqlBridge.Core.Services;

/// Интерфейс для Kafka Consumer
public interface IKafkaConsumerService
{
    Task StartConsumingAsync(CancellationToken cancellationToken);

    void StopConsuming();
}