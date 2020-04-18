using System;

namespace Kafka.Streams.Tests.Integration
{
    public class EmbeddedKafkaCluster
    {
        private int nUM_BROKERS;
        private long v;

        public MockTime time { get; internal set; }

        public EmbeddedKafkaCluster(int nUM_BROKERS)
        {
            this.nUM_BROKERS = nUM_BROKERS;
        }

        public EmbeddedKafkaCluster(int nUM_BROKERS, long v)
            : this(nUM_BROKERS)
        {
            this.v = v;
        }

        internal void CreateTopics(string tOPIC_1, string tOPIC_2, string tOPIC_A, string tOPIC_C, string tOPIC_Y, string tOPIC_Z, string fA_TOPIC, string fOO_TOPIC)
        {
            throw new NotImplementedException();
        }

        internal string bootstrapServers()
        {
            throw new NotImplementedException();
        }

        internal void CreateTopic(string v)
        {
            throw new NotImplementedException();
        }

        internal void deleteTopic(string v)
        {
            throw new NotImplementedException();
        }

        internal void CreateTopic(string iNPUT_STREAM, int v1, int v2)
        {
            throw new NotImplementedException();
        }
    }
}