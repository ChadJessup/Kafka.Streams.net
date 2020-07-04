using System;

namespace Kafka.Streams.Tests.Integration
{
    public class EmbeddedKafkaCluster
    {
        private readonly int numBrokers;
        private readonly long v;

        public MockTime time { get; internal set; }

        public EmbeddedKafkaCluster(int numBrokers)
        {
            this.numBrokers = numBrokers;
        }

        public EmbeddedKafkaCluster(int numBrokers, long v)
            : this(numBrokers)
        {
            this.v = v;
        }

        internal void CreateTopics(params string[] topics)
        {
        }

        internal string bootstrapServers()
        {
            return null;
        }

        internal void CreateTopic(string v)
        {
        }

        internal void deleteTopic(string v)
        {
        }

        internal void CreateTopic(string iNPUT_STREAM, int v1, int v2)
        {
        }
    }
}