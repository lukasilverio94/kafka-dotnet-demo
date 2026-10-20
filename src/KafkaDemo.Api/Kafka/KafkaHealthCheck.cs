using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace KafkaDemo.Api.Kafka;

public class KafkaHealthCheck : IHealthCheck
{
    private readonly KafkaOptions _options;
    public KafkaHealthCheck(IOptions<KafkaOptions> options) => _options = options.Value;

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            using var admin = new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = _options.BootstrapServers }).Build();

            // consulta metadata do cluster- falha se o broker não responder
            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(3));
            return Task.FromResult(HealthCheckResult.Healthy(
                $"Kafka OK ({metadata.Brokers.Count} broker(s))"));
        }
        catch (Exception ex)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy(
                "Kafka indisponível", ex));
        }
    }
}