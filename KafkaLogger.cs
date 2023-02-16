namespace maslogger;

using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text.Json;

public class KafkaLogger : ILogger
{
    private readonly string _categoryName;
    private readonly ProducerConfig _producerConfig;
    private readonly string _topic;
    private readonly string _clientName;
    private readonly string _environment;
    private readonly IProducer<string, string> _producer;
    private readonly LogLevel _minimumLogLevel;

    public KafkaLogger(
        string categoryName,
        string bootstrapServers,
        string topic,
        string clientName,
        string environment,
        LogLevel minimumLogLevel)
    {
        _categoryName = categoryName;
        _producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
        _topic = topic;
        _clientName = clientName;
        _environment = environment;
        _producer = new ProducerBuilder<string, string>(_producerConfig).Build();
        _minimumLogLevel = minimumLogLevel;
    }

    public IDisposable BeginScope<TState>(TState state)
    {
        return null;
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        return logLevel >= _minimumLogLevel;
    }

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception exception,
        Func<TState, Exception, string> formatter)
    {
        if (!IsEnabled(logLevel))
        {
            return;
        }

        var log = new Dictionary<string, object>
        {
            { "Timestamp", DateTime.UtcNow.ToString("o") },
            { "Level", logLevel.ToString() },
            { "Message", formatter(state, exception) },
            { "CategoryName", _categoryName },
            { "Environment", _environment },
            { "ClientName", _clientName },
        };

        _producer.Produce(_topic, new Message<string, string> { Key = Guid.NewGuid().ToString(), Value = JsonSerializer.Serialize(log) });
    }
}

