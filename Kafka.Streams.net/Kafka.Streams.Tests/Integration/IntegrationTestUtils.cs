﻿using Kafka.Common;
using Kafka.Streams.Configs;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests
{
    public static class IntegrationTestUtils
    {
        internal static void WaitUntilFinalKeyValueRecordsReceived<K, V>(StreamsConfig consumerConfig3, string reduceTopic, List<KeyValuePair<K, V>> expectedReducedValues)
        {
        }

        internal static void PurgeLocalStreamsState(StreamsConfig streamsConfiguration)
        {
        }

        internal static void ProduceKeyValuesSynchronously(string globalTableTopic, object p1, object p2, IClock mockTime)
        {
        }

        internal static void ProduceKeyValuesSynchronouslyWithTimestamp(string iNPUT_TOPIC, object p, StreamsConfig producerConfig, long nowAsEpochMilliseconds)
        {
        }

        internal static List<KeyValueTimestamp<K, V>> WaitUntilMinKeyValueWithTimestampRecordsReceived<K, V>(StreamsConfig consumerProperties, string outputTopic, int numMessages, int v)
        {
            return new List<KeyValueTimestamp<K, V>>();
        }

        internal static void ProduceValuesSynchronously(string topic, IEnumerable<string> hashSet, StreamsConfig producerConfig, MockTime mockTime)
        {
            throw new NotImplementedException();
        }

        internal static List<KeyValuePair<string, string>> WaitUntilMinKeyValueRecordsReceived(StreamsConfig consumerConfig, string outputTopic, int v)
        {
            throw new NotImplementedException();
        }

        internal static void VerifyKeyValueTimestamps(StreamsConfig properties, string topic, HashSet<KeyValueTimestamp<string, long>> keyValueTimestamps)
        {
            throw new NotImplementedException();
        }

        internal static void ProduceSynchronously(StreamsConfig producerConfig, bool v1, string topic, int v2, List<KeyValueTimestamp<string, string>> toProduce)
        {
            throw new NotImplementedException();
        }

        internal static void WaitUntilFinalKeyValueTimestampRecordsReceived<K, V>(StreamsConfig consumerProperties, string outputTopic, List<KeyValueTimestamp<K, V>> expectedRecords)
        {
            throw new NotImplementedException();
        }
    }
}
