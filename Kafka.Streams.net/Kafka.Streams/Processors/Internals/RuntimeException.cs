using System;
using System.Runtime.Serialization;

namespace Kafka.Streams
{
    [Serializable]
    public class RuntimeException : Exception
    {
        public RuntimeException()
        {
        }

        public RuntimeException(string message)
            : base(message)
        {
        }

        public RuntimeException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        protected RuntimeException(SerializationInfo LogInformation, StreamingContext context)
            : base(LogInformation, context)
        {
        }
    }
}