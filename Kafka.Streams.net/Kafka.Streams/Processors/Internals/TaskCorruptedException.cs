using System;
using System.Runtime.Serialization;

namespace Kafka.Streams.Processors.Internals
{
    [Serializable]
    internal class TaskCorruptedException : Exception
    {
        public TaskCorruptedException()
        {
        }

        public TaskCorruptedException(string message) : base(message)
        {
        }

        public TaskCorruptedException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected TaskCorruptedException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}