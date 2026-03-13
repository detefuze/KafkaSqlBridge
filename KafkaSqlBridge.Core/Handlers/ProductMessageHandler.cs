using KafkaSqlBridge.Core.Models;
using KafkaSqlBridge.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace KafkaSqlBridge.Core.Handlers
{
    public class ProductMessageHandler : IMessageHandler
    {
        public string Topic => "Loren_Items_Products";

        private readonly IDatabaseService _databaseService;
        private readonly ILogger<ProductMessageHandler> _logger;

        public ProductMessageHandler(IDatabaseService databaseService, ILogger<ProductMessageHandler> logger)
        {
            _databaseService = databaseService;
            _logger = logger;
        }

        public async Task HandleAsync(string messageJson, CancellationToken cancellationToken)
        {
            try
            {
                var product = JsonSerializer.Deserialize<ProductMessage>(messageJson);

                if (product?.IsValid() == true)
                {
                    await _databaseService.ProcessProductMessageAsync(product);
                    _logger.LogInformation("Product {Code} processed", product.product_code);
                }
                else
                {
                    _logger.LogWarning("Invalid product message received");
                }
            }
            catch (JsonException ex) 
            {
                _logger.LogError(ex, "Failed to deserialize message");
                throw;
            }
        }
    }
}
