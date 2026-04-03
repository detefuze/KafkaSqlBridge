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
        public string material_name { get; set; } = string.Empty;
        public int material_type { get; set; }

        public MaterialType enumMaterialType
        {
            get
            {
                return material_type switch
                {
                    1 => MaterialType.Raw,
                    2 => MaterialType.Packaging,
                    3 => MaterialType.SemiFinishedProduct,
                    _ => MaterialType.Unknown
                };
            }
        }

        public bool IsValid()
        {
            return !string.IsNullOrEmpty(material_code) &&
                   !string.IsNullOrEmpty(material_name) &&
                   (enumMaterialType != MaterialType.Unknown);
        }

        public override string ToString()
        {
            return $"Message[{material_code}]: {material_name}: {material_type}";
        }
    }
 
    public enum MaterialType
    {
        Unknown = 0,
        Raw = 1,
        Packaging = 2,
        SemiFinishedProduct = 3
    }
}
