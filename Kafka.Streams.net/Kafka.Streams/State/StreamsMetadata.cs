using Confluent.Kafka;
using System.Collections.Generic;

namespace Kafka.Streams.State
{
    /**
     * Represents the state of an instance (process) in a {@link KafkaStreams} application.
     * It contains the user supplied {@link HostInfo} that can be used by developers to build
     * APIs and services to connect to other instances, the Set of state stores available on
     * the instance and the Set of {@link TopicPartition}s available on the instance.
     * NOTE: This is a point in time view. It may change when rebalances happen.
     */

    public class StreamsMetadata
    {
        /**
         * Sentinel to indicate that the StreamsMetadata is currently unavailable. This can occur during rebalance
         * operations.
         */
        public static StreamsMetadata NOT_AVAILABLE { get; } = new StreamsMetadata(
            new HostInfo("unavailable", -1),
            new HashSet<string>(),
            new HashSet<TopicPartition>());

        private readonly HostInfo hostInfo;
        public HashSet<string> StateStoreNames { get; }
        public HashSet<TopicPartition> TopicPartitions { get; }

        public StreamsMetadata(
            HostInfo hostInfo,
            HashSet<string> stateStoreNames,
            HashSet<TopicPartition> topicPartitions)
        {

            this.hostInfo = hostInfo;
            this.StateStoreNames = stateStoreNames;
            this.TopicPartitions = topicPartitions;
        }

        public string Host => this.hostInfo.host;

        public int Port => this.hostInfo.port;

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
            var that = (StreamsMetadata)o;
            if (!this.hostInfo.Equals(that.hostInfo))
            {
                return false;
            }
            if (!this.StateStoreNames.Equals(that.StateStoreNames))
            {
                return false;
            }
            return this.TopicPartitions.Equals(that.TopicPartitions);

        }

        public override int GetHashCode()
        {
            var result = this.hostInfo.GetHashCode();
            result = 31 * result + this.StateStoreNames.GetHashCode();
            result = 31 * result + this.TopicPartitions.GetHashCode();
            return result;
        }

        public override string ToString()
        {
            return "StreamsMetadata{" +
                    "hostInfo=" + this.hostInfo +
                    ", stateStoreNames=" + this.StateStoreNames +
                    ", topicPartitions=" + this.TopicPartitions +
                    '}';
        }
    }
}
