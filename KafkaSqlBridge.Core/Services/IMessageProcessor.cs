using KafkaSqlBridge.Core.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaSqlBridge.Core.Services;

public interface IMessageProcessor
{
    Task ProcessMessageAsync(ErpMessage message, CancellationToken cancellationToken);
}

