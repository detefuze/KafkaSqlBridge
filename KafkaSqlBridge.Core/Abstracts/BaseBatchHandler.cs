using Confluent.Kafka;
using KafkaSqlBridge.Core.Interfaces;
using KafkaSqlBridge.Core.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace KafkaSqlBridge.Core.Abstracts
{
    public abstract class BaseBatchHandler<T> : IMessageHandler where T : class, IValidatable
    {
        private readonly List<T> _buffer = new();
        private readonly Timer _flushTimer;
        private readonly int _batchSize;
        private readonly object _lock = new();
        private readonly ILogger _logger;

        protected BaseBatchHandler(
            ILogger logger,
            int batchSize = 300, 
            int flushIntervalMs = 1000)  
        { 
            _batchSize = batchSize;
            _flushTimer = new Timer(Flush, null, flushIntervalMs, flushIntervalMs);
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public abstract string Topic { get; }
        protected abstract string GetKey(T item);

        public async Task HandleAsync(string messageJson, CancellationToken cancellationToken)
        {
            T? item = null;

            try
            {
                item = JsonSerializer.Deserialize<T>(messageJson);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Ошибка десериализации");
                return;
            }

            if (!item.IsValid())
            {
                _logger.LogWarning("Получено невалидное сообщение: {message}", messageJson);
                return;
            }

            lock (_lock)
            {
                _buffer.Add(item);
                if (_buffer.Count >= _batchSize)
                {
                    Flush(null);
                }
            }

            await Task.CompletedTask;
        }

        private void Flush(object? state)
        {
            List<T> batch;

            lock (_lock)
            {
                if (_buffer.Count == 0) return;
                batch = new List<T>(_buffer);
                _buffer.Clear();
            }

            var uniqueBatch = batch.GroupBy(GetKey).Select(m => m.Last()).ToList();

            if (uniqueBatch.Count != batch.Count())
            {
                _logger.LogDebug("Дедупликация: {Original} -> {Unique} сообщений",
                batch.Count, uniqueBatch.Count);
            }

            Task.Run(async () =>
            {
                try
                {
                    await ProcessBatchAsync(uniqueBatch);
                    _logger?.LogInformation("Обработано сообщений за один батч: {batchSize}", batch.Count);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Ошибка при обработке batch");
                }
            });
        }

        protected abstract Task ProcessBatchAsync(List<T> batch);
    }
}
