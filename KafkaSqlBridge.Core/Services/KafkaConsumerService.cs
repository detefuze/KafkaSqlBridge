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
    private long _totalMs;


  
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
        _logger.LogInformation("Старт Kafka консьюмера, прослушивание топиков: {Topics}", string.Join(", ", _kafkaSettings.Topics));

        // Подписка на топики из конфигурации
        //_consumer.Subscribe(_kafkaSettings.Topics);

        _cancellationTokenSource = CancellationTokenSource
            .CreateLinkedTokenSource(cancellationToken);

        // Запуск задач для каждого консьюмера
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
                        _logger.LogTrace("Достигнут конец партиции {Topic}:{Partition}", consumeResult.Topic, consumeResult.Partition);
                        continue;
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
        }
    }

    private async Task ProcessConsumeResult(IConsumer<Ignore, string> consumer, ConsumeResult<Ignore, string> consumeResult,
        CancellationToken cancellationToken)
    {
        _stopwatch.Restart();
        
        var topic = consumeResult.Topic;

        //_logger.LogDebug("Получено сообщение из топика {Topic}, партиции [{Partition}] @{Offset}",
        //    consumeResult.Topic,
        //    consumeResult.Partition,
        //    consumeResult.Offset);

        try
        {
            if (_handlers.TryGetValue(topic, out var handler))
            {
                //_logger.LogTrace("Найден обработчик {Handler} для топика {Topic}",
                //    handler.GetType().Name, topic);

                // Передача сообщения в хендлер
                await handler.HandleAsync(consumeResult.Message.Value, cancellationToken);

                if (!_kafkaSettings.EnableAutoCommit)
                {
                    consumer.StoreOffset(consumeResult); // для теста
                    // _consumer.Commit(consumeResult); // сохранение offset
                    _logger.LogTrace("Offset {Offset} сохранен для топика {Topic}", consumeResult.Offset, topic);
                }
            }
            else
            {
                _logger.LogWarning("Нет зарегистрированного обработчика для топика {Topic}", topic);
            }
            _stopwatch.Stop();
            _logger.LogInformation("Полное время обработки сообщения {Topic} : {Offset}: {ElapsedMs} мс",
                        consumeResult.Topic,
                        consumeResult.Offset,
                        _stopwatch.ElapsedMilliseconds);

            _totalMs += _stopwatch.ElapsedMilliseconds;
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
        _cancellationTokenSource?.Cancel();
        _logger.LogInformation("Остановка Kafka консьюмеров...");

        Task.WhenAll(_consumingTasks).Wait(TimeSpan.FromSeconds(10));

        _logger.LogInformation("Среднее время обработки всех сообщений: {totalMS}", _totalMs);
        _totalMs = 0;
    }

    // Освобождение ресурсов
    public void Dispose()
    {
       foreach (var consumer in _consumers)
        {
            consumer.Dispose();
        }
        _cancellationTokenSource?.Dispose();
        GC.SuppressFinalize(this);
    }
}