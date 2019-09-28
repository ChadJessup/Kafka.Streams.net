using System;

namespace Kafka.Streams.Errors
{
    /**
     * Indicates a run time error incurred while trying parse the {@link org.apache.kafka.streams.processor.TaskId task id}
     * from the read string.
     *
     * @see org.apache.kafka.streams.processor.Internals.StreamTask
     */
    public class TaskIdFormatException : StreamsException
    {
        public TaskIdFormatException(string message)
            : base("Task id cannot be parsed correctly" + (message == null ? "" : " from " + message))
        {
        }

        public TaskIdFormatException(string message, Exception throwable)
            :base("Task id cannot be parsed correctly" + (message == null ? "" : " from " + message), throwable)
        {
        }

        public TaskIdFormatException(Exception throwable)
            : base(throwable)
        {
        }
    }
}
