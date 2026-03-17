using KafkaSqlBridge.Core.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaSqlBridge.Core.Interfaces
{
    public interface IDatabaseService
    {
        Task ProcessProductMessageAsync(ProductMessage product);
        Task ProcessMaterialMessageAsync(MaterialMessage product);
    }
}
