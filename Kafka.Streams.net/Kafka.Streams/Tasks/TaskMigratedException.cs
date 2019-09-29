using Kafka.Streams.Tasks;
using System;
using System.Runtime.Serialization;

namespace Kafka.Streams.Processors.Internals
{
    [Serializable]
    public class TaskMigratedException : Exception
    {
        private readonly ProducerFencedException fatal;

        public bool IsFatal { get; }
        public ITask? MigratedTask { get; }

        public TaskMigratedException(ITask task)
            => this.MigratedTask = task;

        public TaskMigratedException()
            => this.MigratedTask = null;

        public TaskMigratedException(string message)
            : base(message)
        {
        }

        public TaskMigratedException(
            ITask task,
            ProducerFencedException fatal,
            bool isFatal)
        {
            this.IsFatal = isFatal;
            this.fatal = fatal;

            this.MigratedTask = task;
        }

        public TaskMigratedException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        protected TaskMigratedException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}