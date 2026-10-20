using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace KafkaDemo.Api.Kafka;

public class KafkaProducerService : IDisposable
{
    private readonly IProducer<Null, string> _producer;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(IOptions<KafkaOptions> options, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        var config = new ProducerConfig { BootstrapServers = options.Value.BootstrapServers };
        _producer = new ProducerBuilder<Null, string>(config).Build();
    }

    public async Task<DeliveryResult<Null, string>> ProduceAsync(string topic, string message, CancellationToken ct = default)
    {
        var result = await _producer.ProduceAsync(topic, new Message<Null, string> { Value = message }, ct);
        _logger.LogInformation("Produzido em {Topic} [{Partition}] offset {Offset}: {Message}",
        result.Topic, result.Partition.Value, result.Offset.Value, message);
        return result;
    }

    public void Dispose() => _producer.Dispose();
}
