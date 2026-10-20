using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace KafkaDemo.Api.Kafka;

public class KafkaConsumerWorker : BackgroundService
{
    private readonly KafkaOptions _options;
    private readonly ILogger<KafkaConsumerWorker> _logger;

    public KafkaConsumerWorker(IOptions<KafkaOptions> options, ILogger<KafkaConsumerWorker> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                GroupId = _options.GroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(_options.Topic);
            _logger.LogInformation("Consumer inscrito no tópico {Topic} (grupo {GroupId})",
                _options.Topic, _options.GroupId);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(stoppingToken);
                        _logger.LogInformation("Consumido de {Topic} [{Partition}] offset {Offset}: {Message}",
                            cr.Topic, cr.Partition.Value, cr.Offset.Value, cr.Message.Value);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Erro ao consumir: {Reason}", ex.Error.Reason);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // shutdown normal
            }
            finally
            {
                consumer.Close();
                _logger.LogInformation("Consumer encerrado.");
            }
        }, stoppingToken);
    }
}