namespace KafkaSqlBridge.Core.Configuration;

public class KafkaSettings
{
    public string BootstrapServers { get; set; } = string.Empty;
    public string GroupId { get; set; } = string.Empty;
    public List<string> Topics { get; set; } = new List<string>();
    public string AutoOffsetReset { get; set; } = string.Empty;
    public bool EnableAutoCommit { get; set; } = false;
}
