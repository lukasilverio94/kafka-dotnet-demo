using KafkaDemo.Api.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace KafkaDemo.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class MessagesController : ControllerBase
{
    private readonly KafkaProducerService _producer;
    private readonly KafkaOptions _options;

    public MessagesController(KafkaProducerService producer, IOptions<KafkaOptions> options)
    {
        _producer = producer;
        _options = options.Value;
    }

    [HttpPost]
    public async Task<IActionResult> Post([FromBody] string message, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(message))
            return BadRequest("Mensagem vazia.");

        var result = await _producer.ProduceAsync(_options.Topic, message, ct);
        return Ok(new
        {
            result.Topic,
            Partition = result.Partition.Value,
            Offset = result.Offset.Value,
            result.Message.Value
        });
    }
}