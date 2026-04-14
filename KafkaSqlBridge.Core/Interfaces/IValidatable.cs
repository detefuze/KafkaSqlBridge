using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaSqlBridge.Core.Interfaces
{
    public interface IValidatable
    {
        bool IsValid();
    }
}
