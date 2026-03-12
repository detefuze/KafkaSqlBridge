namespace KafkaSqlBridge.Core.Configuration;

public class KafkaSettings
{
    public string BootstrapServers { get; set; } = "kafka-bus-test1.alkor.ru:9092";
    public string GroupId { get; set; } = "Loren_Consumer_Bridge_Group";
    public string Topic { get; set; } = "Loren_Items_Products";
    public string AutoOffsetReset { get; set; } = "earliest";
    public bool EnableAutoCommit { get; set; } = false;
}
