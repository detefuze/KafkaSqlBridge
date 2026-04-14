using Confluent.Kafka;
using KafkaSqlBridge.Core.Configuration;
using KafkaSqlBridge.Core.Interfaces;
using KafkaSqlBridge.Core.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;
using System.Text.RegularExpressions;
using KafkaSqlBridge.Core.Handlers;
using System.Diagnostics;

namespace KafkaSqlBridge.Core.Services;

public class KafkaConsumerService : IKafkaConsumerService, IDisposable
{
    private readonly ILogger<KafkaConsumerService> _logger; 
    private readonly KafkaSettings _kafkaSettings; // Конфигурация кафки
    private readonly Dictionary<string, IMessageHandler> _handlers; // Интерфейсы обработки сообщений
    private List<IConsumer<Ignore, string>> _consumers; 
    private List<Task> _consumingTasks;
    private CancellationTokenSource? _cancellationTokenSource;
    private readonly Stopwatch _stopwatch = new(); // счетчик времени обработки сообщения
    


    public KafkaConsumerService(
        ILogger<KafkaConsumerService> logger,
        IOptions<KafkaSettings> kafkaSettings,
        IEnumerable<IMessageHandler> handlers)
    {
        _logger = logger;
        _kafkaSettings = kafkaSettings.Value;
        _handlers = handlers.ToDictionary(handler => handler.Topic);

        _consumingTasks = new List<Task>();
        _consumers = InitializeConsumers();
    }

    // Инициализация консьюмера
    private List<IConsumer<Ignore, string>> InitializeConsumers()
    {
        _consumers = new List<IConsumer<Ignore, string>>();

        foreach (var topic in _handlers.Keys)
        {

            // Конфигурация консьюмера
            var config = new ConsumerConfig()
            {
                BootstrapServers = _kafkaSettings.BootstrapServers,
                GroupId = $"pms-bridge-group-{topic}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = _kafkaSettings.EnableAutoCommit,
                EnableAutoOffsetStore = false,
                AllowAutoCreateTopics = false
            };

            var consumer = new ConsumerBuilder<Ignore, string>(config)
            .SetErrorHandler(OnError)
            .SetLogHandler(OnLog)
            .Build();

            consumer.Subscribe(topic);
            _consumers.Add(consumer);
        }

            return _consumers;
    }
    // Логирование ошибок консьюмера
    private void OnError(IConsumer<Ignore, string> consumer, Error error)
    {
        _logger.LogError("Ошибка: {Reason} (code: {Code})", error.Reason, error.Code);
    }

    // Общее логирование консьюмера
    private void OnLog(IConsumer<Ignore, string> consumer, LogMessage log)
    {
        _logger.LogDebug("Log: {Message} (level: {Level})", log.Message, log.Level);
    }

    public async Task StartConsumingAsync(CancellationToken cancellationToken)
    {
        _stopwatch.Restart();
        _logger.LogInformation("Старт Kafka консьюмера, прослушивание топиков: {Topics}", string.Join(", ", _kafkaSettings.Topics));

        _cancellationTokenSource = CancellationTokenSource
            .CreateLinkedTokenSource(cancellationToken);

        // Запуск задач 
        foreach (var consumer in _consumers)
        {
            var task = Task.Run(() => ConsumeMessages(consumer, _cancellationTokenSource.Token));
            _consumingTasks.Add(task);
        }

        await Task.CompletedTask;
    }

    private async Task ConsumeMessages(IConsumer<Ignore, string> consumer, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Старт Консьюмера. Ожидание сообщений...");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // Таймаут ожидания сообщения 1000 мс
                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(1000));

                    if (consumeResult == null) continue;

                    if (consumeResult.IsPartitionEOF)
                    {
                        _logger.LogInformation("Достигнут конец партиции {Topic}:{Partition}", consumeResult.Topic, consumeResult.Partition);
                        break;
                    }

                    await ProcessConsumeResult(consumer, consumeResult, cancellationToken);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Ошибка получения сообщения: {Error}", ex.Error.Reason);
                    if (ex.Error.IsFatal) break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Неизвестная ошибка при чтении сообщения");
                    await Task.Delay(5000, cancellationToken);
                }
            }
        }
        finally
        {
            _logger.LogInformation("Остановка консьюмера");
            consumer.Close();
            _logger.LogDebug("Время обработки всех сообщений: {totalMS}", _stopwatch.ElapsedMilliseconds);
        }
    }

    private async Task ProcessConsumeResult(IConsumer<Ignore, string> consumer, ConsumeResult<Ignore, string> consumeResult,
        CancellationToken cancellationToken)
    {
        
        var topic = consumeResult.Topic;

        try
        {
            if (_handlers.TryGetValue(topic, out var handler))
            {
                // Передача сообщения в хендлер
                await handler.HandleAsync(consumeResult.Message.Value, cancellationToken);

                if (!_kafkaSettings.EnableAutoCommit)
                {
                    consumer.StoreOffset(consumeResult); 
                    _logger.LogTrace("Offset {Offset} сохранен для топика {Topic}", consumeResult.Offset, topic);
                }
            }
            else
            {
                _logger.LogWarning("Нет зарегистрированного обработчика для топика {Topic}", topic);
            }
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Ошибка JSON десериализации");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ошибка чтения сообщения");
        }
    }

    // Остановка потребления сообщений
    public void StopConsuming()
    {
        _stopwatch.Stop();

        _cancellationTokenSource?.Cancel();
        _logger.LogInformation("Остановка Kafka консьюмеров...");

        Task.WhenAll(_consumingTasks).Wait(TimeSpan.FromSeconds(10));

        _logger.LogInformation("Время обработки всех сообщений: {totalMS}", _stopwatch.ElapsedMilliseconds);
    }

    // Освобождение ресурсов
    public void Dispose()
    {
       foreach (var consumer in _consumers)
        {
            try
            {
                consumer.Commit();
            }
            catch (Exception ex)
            {
                // Логируем, но не прерываем
                _logger?.LogWarning(ex, "Ошибка при коммите offset для consumer'а");
            }
            consumer.Commit();
            consumer.Dispose();
        }
        _cancellationTokenSource?.Dispose();
        GC.SuppressFinalize(this);
    }
}