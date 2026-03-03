namespace KafkaSqlBridge.Core.Models;

public class ErpMessage 
{
    public string MessageId { get; set; } = string.Empty;

    public string OperationType {  get; set; } = string.Empty;

    public DateTime Timestamp { get; set; }

    public string EntityId { get; set; } = string.Empty;

    public string EntityType {  get; set; } = string.Empty;

    public string Payload { get; set; } = string.Empty;

    public bool IsValid()
    {
        return !string.IsNullOrEmpty(MessageId) &&
               !string.IsNullOrEmpty(OperationType) &&
               !string.IsNullOrEmpty(EntityId) &&
               !string.IsNullOrEmpty(EntityType) &&
               !string.IsNullOrEmpty(Payload);
    }

    public override string ToString()
    {
        return $"Message[{MessageId}]: {OperationType} {EntityType}: {EntityId} at {Timestamp:HH:mm:ss}";
    }

}