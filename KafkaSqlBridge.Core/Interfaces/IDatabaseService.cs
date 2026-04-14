using KafkaSqlBridge.Core.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaSqlBridge.Core.Interfaces
{
    public interface IDatabaseService
    {
        Task ProcessProductBatchAsync(List<ProductMessage> batch);
        Task ProcessMaterialBatchAsync(List<MaterialMessage> batch);
        //Task ProcessProductMessageAsync(ProductMessage product);
        //Task ProcessMaterialMessageAsync(MaterialMessage material);
    }
}
