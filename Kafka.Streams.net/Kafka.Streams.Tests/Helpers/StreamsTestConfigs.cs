using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Helpers
{
    public static class StreamsTestConfigs
    {
        public static StreamsConfig GetStandardConfig(
            string applicationId = "stream-thread-test",
            bool enableEoS = false,
            int numberOfMockBrokers = 3,
            int numberOfThreads = 2)
            => new StreamsConfig(new Dictionary<string, string?>
                {
                    { "test.mock.num.brokers", numberOfMockBrokers.ToString() },
                    {  StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIGConfig, "3" },
                    { StreamsConfig.ProcessingGuaranteeConfig, enableEoS ? StreamsConfig.ExactlyOnceConfig : StreamsConfig.AtLeastOnceConfig },
                })
            {
                ClientId = "clientId",
                BootstrapServers = "localhost:9092",
                NumberOfStreamThreads = numberOfThreads,
                ApplicationId = applicationId,
                DefaultTimestampExtractorType = typeof(MockTimestampExtractor),
                StateStoreDirectory = TestUtils.GetTempDirectory(),
                GroupId = "testGroupId",
            };

        public static StreamsConfig GetStandardConfig(
            string applicationId,
            string bootstrapServers,
            Type keySerdeClassType,
            Type valueSerdeClassType,
            StreamsConfig additional)
        {
            StreamsConfig props = GetStandardConfig(applicationId, enableEoS: false);

            props.BootstrapServers = bootstrapServers;
            props.DefaultKeySerdeType = keySerdeClassType;
            props.DefaultValueSerdeType = valueSerdeClassType;
            props.StateStoreDirectory = TestUtils.GetTempDirectory();
            props.SetAll(additional);

            return props;
        }

        public static StreamsConfig GetStandardConfig<K, V>(
            ISerde<K> keyDeserializer,
            ISerde<V> valueDeserializer)
        {
            return GetStandardConfig(
                    Guid.NewGuid().ToString(),
                    "localhost:9091",
                    keyDeserializer.GetType(),
                    valueDeserializer.GetType(),
                    GetStandardConfig());
        }

        public static StreamsConfig GetStandardConfig(string applicationId)
        {
            return GetStandardConfig(
                applicationId,
                "localhost:9091",
                Serdes.ByteArray().GetType(),
                Serdes.ByteArray().GetType(),
            new StreamsConfig());
        }
    }
}
