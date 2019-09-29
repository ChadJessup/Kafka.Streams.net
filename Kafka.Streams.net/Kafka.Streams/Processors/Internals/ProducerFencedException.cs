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
            : base(new Error(ErrorCode.InvalidProducerEpoch, message))
        {
        }

        public ProducerFencedException(
            string message,
            Exception innerException)
            : base(new Error(ErrorCode.InvalidProducerEpoch, message), innerException)
        {
        }
    }
}