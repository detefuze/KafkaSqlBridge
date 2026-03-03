namespace KafkaSqlBridge.Core.Configuration;

public class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string GroupId { get; set; } = "kafka-sql-bridge-group";
    public string Topic { get; set; } = "erp-events";
    public string AutoOffsetReset { get; set; } = "earliest";
    public bool EnableAutoCommit { get; set; } = false;
}
