using Confluent.Kafka;
using KafkaSqlBridge.Core.Configuration;
using KafkaSqlBridge.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace KafkaSqlBridge.Core.Services;

public class KafkaConsumerService : IKafkaConsumerService, IDisposable
{
    private readonly ILogger<KafkaConsumerService> _logger; // логгер
    private readonly KafkaSettings _kafkaSettings; // конфигурация кафки
    private readonly IMessageProcessor _messageProcessor; // интерфейс обработки сообщений
    private IConsumer<Ignore, string> _consumer;
    private Task? _consumingTask;
    private CancellationTokenSource? _cancellationTokenSource;

  
    /// Конструктор с DI
    public KafkaConsumerService(
        ILogger<KafkaConsumerService> logger,
        IOptions<KafkaSettings> kafkaSettings,
        IMessageProcessor messageProcessor)
    {
        _logger = logger;
        _kafkaSettings = kafkaSettings.Value;
        _messageProcessor = messageProcessor;

        InitializeConsumer();
    }

    // Метод инициализации консьюмера
    private void InitializeConsumer()
    {
        // Конфигурация консьюмера
        var config = new ConsumerConfig()
        {
            BootstrapServers = _kafkaSettings.BootstrapServers,
            GroupId = _kafkaSettings.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = _kafkaSettings.EnableAutoCommit,
            EnableAutoOffsetStore = false,
            AllowAutoCreateTopics = false
        };
        _consumer = new ConsumerBuilder<Ignore, string>(config)
            .SetErrorHandler(OnError)
            .SetLogHandler(OnLog)
            .Build();
    }
    // Логирование ошибок консьюмера
    private void OnError(IConsumer<Ignore, string> consumer, Error error)
    {
        _logger.LogError("Kafka error: {Reason} (code: {Code})", error.Reason, error.Code);
    }

    // Общее логирование консьюмера
    private void OnLog(IConsumer<Ignore, string> consumer, LogMessage log)
    {
        _logger.LogDebug("Kafka log: {Message} (level: {Level})", log.Message, log.Level);
    }

    public async Task StartConsumingAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting Kafka consumer for topic: {Topic}", _kafkaSettings.Topic);

        // Подписка на топик
        _consumer.Subscribe(_kafkaSettings.Topic);

        _cancellationTokenSource = CancellationTokenSource
            .CreateLinkedTokenSource(cancellationToken);

        // Фоновая задача потребления сообщений
        _consumingTask = Task.Run(() => ConsumeMessages(_cancellationTokenSource.Token));

        // Возвращаем завершенную задачу
        await Task.CompletedTask;
    }

    private async Task ConsumeMessages(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Consumer started. Waiting for messages...");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Таймаут ожидания сообщения 1000 мс
                    var consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(1000));

                    if (consumeResult == null) continue;

                    if (consumeResult.IsPartitionEOF)
                    {
                        _logger.LogTrace("Reached end of partition");
                        continue;
                    }

                    await ProcessConsumeResult(consumeResult, cancellationToken);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Consume error: {Error}", ex.Error.Reason);
                    if (ex.Error.IsFatal) break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error in consumer loop");
                    await Task.Delay(5000, cancellationToken);
                }
            }
        }
        finally
        {
            _logger.LogInformation("Consumer stopped");
            _consumer.Close();
        }
    }

    private async Task ProcessConsumeResult(ConsumeResult<Ignore, string> consumeResult,
        CancellationToken cancellationToken)
    {
        _logger.LogDebug("Received message from {Topic}[{Partition}]@{Offset}",
            consumeResult.Topic,
            consumeResult.Partition,
            consumeResult.Offset);

        try
        {
            // Десериализация JSON-строки в объект ErpMessage
            var message = JsonSerializer.Deserialize<ErpMessage>(consumeResult.Message.Value);

            if (message == null)
            {
                _logger.LogWarning("Failed to deserialize message: {RawMessage}",
                    consumeResult.Message.Value);
                return;
            }

            if (!message.IsValid())
            {
                _logger.LogWarning("Invalid message received: {MessageId}", message.MessageId);
                return;
            }

            _logger.LogInformation("Processing: {Message}", message.ToString());

            // Передача сообщения процессору для дальнейшей обработки (ConsoleMessageProcessor)
            await _messageProcessor.ProcessMessageAsync(message, cancellationToken);

            if (!_kafkaSettings.EnableAutoCommit)
            {
                _consumer.StoreOffset(consumeResult);
                _logger.LogTrace("Offset stored for {Offset}", consumeResult.Offset);
            }
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "JSON deserialization error");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message");
        }
    }

    // Метод остановки потребления сообщений
    public void StopConsuming()
    {
        _cancellationTokenSource?.Cancel();
        _logger.LogInformation("Stop requested for Kafka consumer");
    }

    // Метод освобождения ресурсов
    public void Dispose()
    {
        _consumer?.Dispose();
        _cancellationTokenSource?.Dispose();
        GC.SuppressFinalize(this);
    }
}