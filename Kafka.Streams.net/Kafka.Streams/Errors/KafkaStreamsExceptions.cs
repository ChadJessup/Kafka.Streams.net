using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Kafka.Streams.Errors
{
    public class KafkaStreamsExceptions : Exception
    {
        public KafkaStreamsExceptions()
        {
        }

        public KafkaStreamsExceptions(string message)
            : base(message)
        {
        }

        public KafkaStreamsExceptions(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        protected KafkaStreamsExceptions(SerializationInfo LogInformation, StreamingContext context)
            : base(LogInformation, context)
        {
        }
    }
}
