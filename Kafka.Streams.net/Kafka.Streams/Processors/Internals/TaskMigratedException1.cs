using System;
using System.Runtime.Serialization;

namespace Kafka.Streams.Processor.Internals
{
    [Serializable]
    internal class TaskMigratedException : Exception
    {
        private StreamTask task;

        public TaskMigratedException()
        {
        }

        public TaskMigratedException(string message) : base(message)
        {
        }

        public TaskMigratedException(StreamTask task)
        {
            this.task = task;
        }

        public TaskMigratedException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected TaskMigratedException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}