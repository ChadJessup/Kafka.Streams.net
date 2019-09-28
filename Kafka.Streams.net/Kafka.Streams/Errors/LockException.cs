using System;

namespace Kafka.Streams.Errors
{
    /**
     * Indicates that the state store directory lock could not be acquired because another thread holds the lock.
     *
     * @see org.apache.kafka.streams.processor.IStateStore
     */
    public class LockException : StreamsException
    {
        public LockException(string message)
            : base(message)
        {
        }

        public LockException(string message, Exception throwable)
            : base(message, throwable)
        {
        }

        public LockException(Exception throwable)
            : base(throwable)
        {
        }
    }
}