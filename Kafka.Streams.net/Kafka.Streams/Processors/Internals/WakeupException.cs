
using System;
using System.Runtime.Serialization;

namespace Kafka.Streams.Processors.Internals
{
    [Serializable]
    public class WakeupException : Exception
    {
        public WakeupException()
        {
        }

        public WakeupException(string message) : base(message)
        {
        }

        public WakeupException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected WakeupException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
