
using System;
using System.Runtime.Serialization;

namespace Kafka.Streams.Processors.Internals
{
    [Serializable]
    internal class CommitFailedException : Exception
    {
        public CommitFailedException()
        {
        }

        public CommitFailedException(string message) : base(message)
        {
        }

        public CommitFailedException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected CommitFailedException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
