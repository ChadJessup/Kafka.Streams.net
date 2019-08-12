using System;
using System.Runtime.Serialization;

namespace Kafka.Streams.Processor.Internals
{
    [Serializable]
    internal class OverlappingFileLockException : Exception
    {
        public OverlappingFileLockException()
        {
        }

        public OverlappingFileLockException(string message) : base(message)
        {
        }

        public OverlappingFileLockException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected OverlappingFileLockException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}