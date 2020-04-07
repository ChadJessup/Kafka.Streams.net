
using System;

namespace Kafka.Streams.Errors
{
    /**
     * Indicates a pre run time error occurred while parsing the {@link org.apache.kafka.streams.Topology logical topology}
     * to construct the {@link org.apache.kafka.streams.processor.Internals.ProcessorTopology physical processor topology}.
     */
    public class TopologyException : StreamsException
    {
        public TopologyException(string message)
            : base("Invalid topology" + (message == null ? "" : ": " + message))
        {
        }

        public TopologyException(string message, Exception throwable)
            : base("Invalid topology" + (message == null ? "" : ": " + message), throwable)
        {
        }

        public TopologyException(Exception throwable)
            : base(throwable)
        {
        }
    }
}
