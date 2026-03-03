namespace KafkaSqlBridge.Core.Configuration;

public class PMSDatabaseSettings
{
    public string ConnectionString { get; set; } = string.Empty;
    public string TableName { get; set; } = "ErpMessages";
    public int CommandTimeout { get; set; } = 30;
}
