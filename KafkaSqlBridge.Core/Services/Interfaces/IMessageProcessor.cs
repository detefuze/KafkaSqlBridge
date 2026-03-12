using KafkaSqlBridge.Core.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaSqlBridge.Core.Services.Interfaces;

public interface IMessageProcessor
{
    Task ProcessProductMessageAsync(ProductMessage message, CancellationToken cancellationToken);
}

