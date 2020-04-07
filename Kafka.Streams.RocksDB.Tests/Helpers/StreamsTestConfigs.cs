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
            => new StreamsConfig(new Dictionary<string, string>
                {
                    { StreamsConfigPropertyNames.ClientId, "clientId" },
                    { "test.mock.num.brokers", numberOfMockBrokers.ToString() },
                    { StreamsConfigPropertyNames.BootstrapServers, "localhost:9092" },
                    { StreamsConfigPropertyNames.NumberOfStreamThreads, numberOfThreads.ToString() },
                    { StreamsConfigPropertyNames.ApplicationId, applicationId },
                    { StreamsConfigPropertyNames.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3" },
                    { StreamsConfigPropertyNames.DefaultTimestampExtractorClass, typeof(MockTimestampExtractor).FullName },
                    { StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().FullName },
                    { StreamsConfigPropertyNames.ProcessingGuarantee, enableEoS? StreamsConfigPropertyNames.ExactlyOnce : StreamsConfigPropertyNames.AtLeastOnce },
                    { StreamsConfigPropertyNames.GroupId, "testGroupId" },
                });

        public static StreamsConfig GetStandardConfig(
            string applicationId,
            string bootstrapServers,
            string keySerdeClassName,
            string valueSerdeClassName,
            StreamsConfig additional)
        {
            StreamsConfig props = GetStandardConfig(applicationId, enableEoS: false);

            props.Set(StreamsConfigPropertyNames.BootstrapServers, bootstrapServers);
            props.Set(StreamsConfigPropertyNames.DefaultKeySerdeClass, keySerdeClassName);
            props.Set(StreamsConfigPropertyNames.DefaultValueSerdeClass, valueSerdeClassName);
            props.Set(StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.GetTempDirectory().FullName);
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
                    keyDeserializer.GetType().FullName,
                    valueDeserializer.GetType().FullName,
                    GetStandardConfig());
        }

        public static StreamsConfig GetStandardConfig(string applicationId)
        {
            return GetStandardConfig(
                applicationId,
                "localhost:9091",
                Serdes.ByteArray().GetType().FullName,
                Serdes.ByteArray().GetType().FullName,
            new StreamsConfig());
        }
    }
}
