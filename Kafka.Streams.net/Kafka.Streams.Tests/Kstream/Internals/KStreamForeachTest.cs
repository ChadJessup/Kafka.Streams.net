using Kafka.Streams;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Tests.Helpers;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamForeachTest
    {

        private string topicName = "topic";
        private ConsumerRecordFactory<int, string> recordFactory = new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String());
        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

        [Fact]
        public void testForeach()
        {
            // Given
            List<KeyValuePair<int, string>> inputRecords = Array.AsReadOnly(
                KeyValuePair.Create(0, "zero"),
                KeyValuePair.Create(1, "one"),
                KeyValuePair.Create(2, "two"),
                KeyValuePair.Create(3, "three")
            );

            List<KeyValuePair<int, string>> expectedRecords = Array.AsReadOnly(
                KeyValuePair.Create(0, "TimeSpan.Zero"),
                KeyValuePair.Create(2, "ONE"),
                KeyValuePair.Create(4, "TWO"),
                KeyValuePair.Create(6, "THREE")
            );

            var actualRecords = new List<KeyValuePair<int, string>>();
            ForeachAction<int, string> action =
                (key, value) => actualRecords.Add(KeyValuePair.Create(key * 2, value.toUppercase(Locale.ROOT)));

            // When
            var builder = new StreamsBuilder();
            IKStream<K, V> stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
            stream.ForEach(action);

            // Then
            var driver = new TopologyTestDriver(builder.Build(), props);
            foreach (KeyValuePair<int, string> record in inputRecords)
            {
                driver.PipeInput(recordFactory.Create(topicName, record.Key, record.Value));
            }

            Assert.Equal(expectedRecords.Count, actualRecords.Count);
            for (var i = 0; i < expectedRecords.Count; i++)
            {
                KeyValuePair<int, string> expectedRecord = expectedRecords[i];
                KeyValuePair<int, string> actualRecord = actualRecords[i];
                Assert.Equal(expectedRecord, actualRecord);
            }
        }

        [Fact]
        public void testTypeVariance()
        {
            ForeachAction<int, object> consume = (key, value) => { };

            new StreamsBuilder()
                .Stream<int, string>("emptyTopic")
                .ForEach(consume);
        }
    }
}
