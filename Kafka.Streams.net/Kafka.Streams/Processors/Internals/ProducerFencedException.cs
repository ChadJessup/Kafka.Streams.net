using Confluent.Kafka;
using System;

namespace Kafka.Streams.Processors.Internals
{
    [Serializable]
    public class ProducerFencedException : KafkaException
    {
        public ProducerFencedException()
            : base(ErrorCode.InvalidProducerEpoch)
        {
        }

        public ProducerFencedException(string message)
            : base(new Confluent.Kafka.Error(ErrorCode.InvalidProducerEpoch, message))
        {
        }

        public ProducerFencedException(
            string message,
            Exception innerException)
            : base(new Confluent.Kafka.Error(ErrorCode.InvalidProducerEpoch, message), innerException)
        {
        }
    }
}
