using Kafka.Streams.Configs;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests
{
    public static class IntegrationTestUtils
    {
        internal static void WaitUntilFinalKeyValueRecordsReceived<K, V>(StreamsConfig consumerConfig3, string reduceTopic, List<KeyValuePair<K, V>> expectedReducedValues)
        {
            throw new NotImplementedException();
        }

        internal static void PurgeLocalStreamsState(StreamsConfig streamsConfiguration)
        {
            throw new NotImplementedException();
        }

        internal static void ProduceKeyValuesSynchronously(string globalTableTopic, object p1, object p2, MockTime mockTime)
        {
            throw new NotImplementedException();
        }
    }
}
