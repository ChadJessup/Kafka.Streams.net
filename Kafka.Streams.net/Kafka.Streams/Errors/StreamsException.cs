using System;
using Confluent.Kafka;

namespace Kafka.Streams.Errors
{
    /**
     * {@link StreamsException} is the top-level exception type generated by Kafka Streams.
     */
    public class StreamsException : KafkaException
    {
        private readonly string message = "";
        private readonly Exception exception;

        public StreamsException(Error error)
            : base(error)
        {
        }

        public StreamsException(ErrorCode code)
            : base(code)
        {
        }

        public StreamsException(Exception exception)
            : base(ErrorCode.Unknown)
        {
            this.exception = exception;
        }

        public StreamsException(string message)
            : base(ErrorCode.Unknown)
        {
            this.message = message;
        }

        public StreamsException(string message, Exception exception)
            : base(ErrorCode.Unknown)
        {
            this.message = message;
        }

        public StreamsException(Error error, Exception innerException)
            : base(error, innerException)
        {
        }
    }
}