
using System;

namespace Kafka.Streams.State
{
    /**
     * Represents a user defined endpoint in a {@link org.apache.kafka.streams.KafkaStreams} application.
     * Instances of this can be obtained by calling one of:
     *  {@link KafkaStreams#allMetadata()}
     *  {@link KafkaStreams#allMetadataForStore(string)}
     *  {@link KafkaStreams#metadataForKey(string, object, StreamPartitioner)}
     *  {@link KafkaStreams#metadataForKey(string, object, Serializer)}
     *
     *  The HostInfo is constructed during Partition Assignment
     *  see {@link StreamsPartitionAssignor}
     *  It is extracted from the config {@link org.apache.kafka.streams.StreamsConfig#APPLICATION_SERVER_CONFIG}
     *
     *  If developers wish to expose an endpoint in their KafkaStreams applications they should provide the above
     *  config.
     */
    public class HostInfo
    {
        public string host { get; }
        public int port { get; }

        public HostInfo(string host,
                        int port)
        {
            this.host = host;
            this.port = port;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var hostInfo = (HostInfo)o;
            return this.port == hostInfo.port && this.host.Equals(hostInfo.host);
        }

        public override int GetHashCode()
        {
            var result = this.host.GetHashCode();
            result = 31 * result + this.port;
            return result;
        }

        public override string ToString()
        {
            return "HostInfo{" +
                    "host=\'" + this.host + '\'' +
                    ", port=" + this.port +
                    '}';
        }

        internal static HostInfo BuildFromEndpoint(string endPoint)
        {
            throw new NotImplementedException();
        }
    }
}
