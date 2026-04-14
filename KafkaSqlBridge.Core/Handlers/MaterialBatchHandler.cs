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
    public class MaterialBatchHandler : BaseBatchHandler<MaterialMessage>
    {
        private readonly IDatabaseService _databaseService;

        public MaterialBatchHandler(IDatabaseService databaseService, ILogger<MaterialBatchHandler> logger): base(logger) 
        { _databaseService = databaseService; }

        public override string Topic => "Loren_Items_Materials";

        protected override async Task ProcessBatchAsync(List<MaterialMessage> batch)
        {
            await _databaseService.ProcessMaterialBatchAsync(batch);
        }

        protected override string GetKey(MaterialMessage item)
        {
            return item.material_code;
        }
    }
}
