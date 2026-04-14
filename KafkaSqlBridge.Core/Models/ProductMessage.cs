namespace KafkaSqlBridge.Core.Models;
using KafkaSqlBridge.Core.Interfaces;

public class ProductMessage : IValidatable
{
    public string product_code { get; set; } = string.Empty;

    public string product_name { get; set; } = string.Empty;

    public int PCS_CA { get; set; }

    public decimal CA_massa_n { get; set; }

    public decimal CA_massa_b { get; set; }

    public string wip_code { get; set; } = string.Empty;

    public bool IsValid()
    {
        return !string.IsNullOrEmpty(product_code) &&
               !string.IsNullOrEmpty(product_name);
    }

    public override string ToString()
    {
        return $"Message[{product_code}]: {product_name}: {PCS_CA}, {CA_massa_n}, {CA_massa_b}, {wip_code}";
    }

}