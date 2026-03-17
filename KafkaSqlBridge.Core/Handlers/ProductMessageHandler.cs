using KafkaSqlBridge.Core.Models;
using KafkaSqlBridge.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace KafkaSqlBridge.Core.Handlers
{
    public class ProductMessageHandler : IMessageHandler
    {
        public string Topic => "Loren_Items_Products";

        private readonly IDatabaseService _databaseService;
        private readonly ILogger<ProductMessageHandler> _logger;
        private readonly Stopwatch _stopwatch = new();

        public ProductMessageHandler(IDatabaseService databaseService, ILogger<ProductMessageHandler> logger)
        {
            _databaseService = databaseService;
            _logger = logger;
        }

        public async Task HandleAsync(string messageJson, CancellationToken cancellationToken)
        {
            _stopwatch.Restart();
            try
            {
                var product = JsonSerializer.Deserialize<ProductMessage>(messageJson);

                if (product?.IsValid() == true)
                {
                    await _databaseService.ProcessProductMessageAsync(product);

                    _stopwatch.Stop();

                    _logger.LogInformation("Сообщение {ProductCode} обработано успешно за {ElapsedMs} мс", 
                        product.product_code,
                        _stopwatch.ElapsedMilliseconds);      
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
