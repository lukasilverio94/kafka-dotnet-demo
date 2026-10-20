namespace KafkaDemo.Api.Kafka;

public class KafkaOptions
{
    public const string SectionName = "Kafka";
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "test-topic";
    public string GroupId { get; set; } = "kafka-demo-api-group";
}
