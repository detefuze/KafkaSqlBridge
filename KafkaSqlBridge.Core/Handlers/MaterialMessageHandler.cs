using Confluent.Kafka;
using KafkaSqlBridge.Core.Interfaces;
using KafkaSqlBridge.Core.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace KafkaSqlBridge.Core.Handlers
{
    public class MaterialMessageHandler : IMessageHandler
    {
        public string Topic => "Loren_Items_Materials";

        private readonly IDatabaseService _databaseService;
        private readonly ILogger<MaterialMessageHandler> _logger;
        private readonly Stopwatch _stopwatch = new();

        public MaterialMessageHandler(IDatabaseService databaseService, ILogger<MaterialMessageHandler> logger)
        {
            _databaseService = databaseService;
            _logger = logger;
        }

        public async Task HandleAsync(string messageJson, CancellationToken cancellationToken)
        {
            //_stopwatch.Restart();
            try
            {
                var material = JsonSerializer.Deserialize<MaterialMessage>(messageJson);

                if (material?.IsValid() == true)
                {
                    await _databaseService.ProcessMaterialMessageAsync(material);

                    //_stopwatch.Stop();

                    //_logger.LogInformation("Сообщение {MaterialCode} обработано за {ElapsedMs} мс",
                    //    material.material_code,
                    //    _stopwatch.ElapsedMilliseconds);
                }
                else
                {
                    _logger.LogWarning("Получено невалидное сообщение: {Message}", messageJson);
                }
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Не получилось десериализовать сообщение");
                throw;
            }
        }
    }
}
