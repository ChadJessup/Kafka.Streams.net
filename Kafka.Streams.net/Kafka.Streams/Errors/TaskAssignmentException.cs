using System;

namespace Kafka.Streams.Errors
{
    /**
     * Indicates a run time error incurred while trying to assign
     * {@link org.apache.kafka.streams.processor.Internals.StreamTask stream tasks} to
     * {@link org.apache.kafka.streams.processor.Internals.StreamThread threads}.
     */
    public class TaskAssignmentException : StreamsException
    {
        public TaskAssignmentException(string message)
            : base(message)
        {
        }

        public TaskAssignmentException(string message, Exception throwable)
            : base(message, throwable)
        {
        }

        public TaskAssignmentException(Exception throwable)
            : base(throwable)
        {
        }
    }
}
