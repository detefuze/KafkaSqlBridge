using System.Text.Json;
using Confluent.Kafka;

Console.WriteLine("=== Тестовый Kafka Producer ===");

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};

using var producer = new ProducerBuilder<Null, string>(config).Build();

Console.WriteLine("Вводите сообщения для отправки в Kafka (топик: erp-events)");
Console.WriteLine("Введите 'exit' для выхода\n");

var messageCount = 0;

while (true)
{
    Console.Write("Сообщение: ");
    var text = Console.ReadLine();

    if (text?.ToLower() == "exit")
        break;

    try
    {
        messageCount++;
        var testMessage = new
        {
            MessageId = Guid.NewGuid().ToString(),
            OperationType = "INSERT",
            EntityType = "Test",
            EntityId = $"TEST-{messageCount:000}",
            Timestamp = DateTime.UtcNow,
            Payload = $"{{ \"text\": \"{text}\", \"number\": {messageCount} }}"
        };

        var json = JsonSerializer.Serialize(testMessage);
        var message = new Message<Null, string> { Value = json };

        var result = await producer.ProduceAsync("erp-events", message);

        Console.WriteLine($"✓ Отправлено! ID: {testMessage.MessageId}");
        Console.WriteLine($"  Offset: {result.Offset}, Partition: {result.Partition}");
        Console.WriteLine();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"✗ Ошибка: {ex.Message}");
    }
}

Console.WriteLine("Producer остановлен");