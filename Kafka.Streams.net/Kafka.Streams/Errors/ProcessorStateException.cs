
using System;

namespace Kafka.Streams.Errors
{
    /**
     * Indicates a processor state operation (e.g. put, get) has failed.
     *
     * @see org.apache.kafka.streams.processor.IStateStore
     */
    public class ProcessorStateException : StreamsException
    {
        public ProcessorStateException(string message)
            : base(message)
        {
        }

        public ProcessorStateException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public ProcessorStateException(Exception innerException)
            : base(innerException)
        {
        }
    }
}
