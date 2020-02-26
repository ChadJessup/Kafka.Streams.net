using Kafka.Streams.Configs;
using System.Collections.Generic;

namespace Kafka.Streams.Tests
{
    public static class StreamsTestConfigs
    {
        public static StreamsConfig GetStandardConfig(string applicationId = "stream-thread-test", bool enableEoS = false)
            => new StreamsConfig(new Dictionary<string, string>
                {
                    { StreamsConfigPropertyNames.ApplicationId, applicationId },
                    { StreamsConfigPropertyNames.BootstrapServers, "localhost:2171" },
                    { StreamsConfigPropertyNames.BUFFERED_RECORDS_PER_PARTITION_CONFIG, "3" },
                    { StreamsConfigPropertyNames.DefaultTimestampExtractorClass, typeof(MockTimestampExtractor).FullName },
                    { StreamsConfigPropertyNames.STATE_DIR_CONFIG, TestUtils.GetTempDirectory() },
                    { StreamsConfigPropertyNames.ProcessingGuarantee, enableEoS? StreamsConfigPropertyNames.ExactlyOnce : StreamsConfigPropertyNames.AtLeastOnce },
                });
    }
}
