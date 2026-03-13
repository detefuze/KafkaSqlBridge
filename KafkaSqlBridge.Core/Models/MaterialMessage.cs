using Microsoft.IdentityModel.Tokens;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaSqlBridge.Core.Models
{
    public class MaterialMessage
    {
        public string material_code { get; set; } = string.Empty;
        public string material_name { get; set; } = String.Empty;
        public int material_type { get; set; }

        public bool IsValid()
        {
            return !string.IsNullOrEmpty(material_code) &&
                   !string.IsNullOrEmpty(material_name);
        }

        public override string ToString()
        {
            return $"Message[{material_code}]: {material_name}: {material_type}";
        }
    }
}
