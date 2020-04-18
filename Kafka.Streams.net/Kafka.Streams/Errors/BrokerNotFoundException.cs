
using System;
using Confluent.Kafka;

namespace Kafka.Streams.Errors
{


    /**
     * Indicates that none of the specified {@link org.apache.kafka.streams.StreamsConfig#BootstrapServersConfig brokers}
     * could be found.
     *
     * @see org.apache.kafka.streams.StreamsConfig
     */
    public class BrokerNotFoundException : StreamsException
    {
        public BrokerNotFoundException(string message)
            : base(message)
        {
        }

        public BrokerNotFoundException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
