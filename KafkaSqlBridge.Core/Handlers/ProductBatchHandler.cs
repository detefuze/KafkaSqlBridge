using KafkaSqlBridge.Core.Abstracts;
using KafkaSqlBridge.Core.Interfaces;
using KafkaSqlBridge.Core.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaSqlBridge.Core.Handlers
{
    public class ProductBatchHandler : BaseBatchHandler<ProductMessage>
    {
        private readonly IDatabaseService _databaseService;

        public ProductBatchHandler(IDatabaseService databaseService, ILogger<ProductBatchHandler> logger) : base(logger)
        {
            _databaseService = databaseService;
        }

        public override string Topic => "Loren_Items_Products";

        protected override async Task ProcessBatchAsync(List<ProductMessage> batch)
        {
            await _databaseService.ProcessProductBatchAsync(batch);
        }

        protected override string GetKey(ProductMessage item)
        {
            return item.product_code;
        }
    }
}
