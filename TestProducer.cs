using System.Text.Json;
using Confluent.Kafka;

// Быстрый тестовый Producer
class TestProducer
{
    static async Task Main()
    {
        Console.WriteLine("=== Тестовый Producer для Kafka SQL Bridge ===");

        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        var messages = new[]
        {
            new ErpMessage
            {
                MessageId = Guid.NewGuid().ToString(),
                OperationType = "INSERT",
                EntityType = "Order",
                EntityId = "ORD-001",
                Timestamp = DateTime.UtcNow,
                Payload = "{\"orderId\": \"ORD-001\", \"amount\": 1000}"
            },
            new ErpMessage
            {
                MessageId = Guid.NewGuid().ToString(),
                OperationType = "UPDATE",
                EntityType = "Product",
                EntityId = "PRD-001",
                Timestamp = DateTime.UtcNow,
                Payload = "{\"productId\": \"PRD-001\", \"price\": 99.99}"
            },
            new ErpMessage
            {
                MessageId = Guid.NewGuid().ToString(),
                OperationType = "DELETE",
                EntityType = "Customer",
                EntityId = "CUST-001",
                Timestamp = DateTime.UtcNow,
                Payload = "{\"customerId\": \"CUST-001\"}"
            }
        };

        foreach (var message in messages)
        {
            var json = JsonSerializer.Serialize(message);
            var kafkaMessage = new Message<Null, string> { Value = json };

            try
            {
                var result = await producer.ProduceAsync("erp-events", kafkaMessage);
                Console.WriteLine($"✓ Отправлено: {message}");
                Console.WriteLine($"  Partition: {result.Partition}, Offset: {result.Offset}\n");
                await Task.Delay(1000);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Ошибка: {ex.Message}");
            }
        }

        Console.WriteLine("\nТест завершен");
    }
}