
using System;
using Confluent.Kafka;

namespace Kafka.Streams.Errors
{


    /**
     * Indicates that none of the specified {@link org.apache.kafka.streams.StreamsConfig#BOOTSTRAP_SERVERS_CONFIG brokers}
     * could be found.
     *
     * @see org.apache.kafka.streams.StreamsConfig
     */
    public BrokerNotFoundException : StreamsException
    {
        public BrokerNotFoundException(Confluent.Kafka.Error error) : base(error)
        {
        }

        public BrokerNotFoundException(ErrorCode code) : base(code)
        {
        }

        public BrokerNotFoundException(Confluent.Kafka.Error error, Exception innerException) : base(error, innerException)
        {
        }
    }
}