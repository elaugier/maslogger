namespace maslogger;

using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

public class KafkaLoggerProvider : ILoggerProvider
{
    private readonly ProducerConfig _producerConfig;
    private readonly string _topic;
    private readonly string _clientName;
    private readonly string _environment;
    private readonly IProducer<string, string> _producer;
    private readonly LogLevel _minimumLogLevel;

    public KafkaLoggerProvider(
        string bootstrapServers,
        string topic,
        string clientName,
        string environment,
        LogLevel minimumLogLevel)
    {
        _producerConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
        _topic = topic;
        _clientName = clientName;
        _environment = environment;
        _producer = new ProducerBuilder<string, string>(_producerConfig).Build();
        _minimumLogLevel = minimumLogLevel;
    }

    public ILogger CreateLogger(string categoryName)
    {
        return new KafkaLogger(categoryName, _producerConfigs, _topic, _clientName, _environment, _minimumLogLevel);
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
