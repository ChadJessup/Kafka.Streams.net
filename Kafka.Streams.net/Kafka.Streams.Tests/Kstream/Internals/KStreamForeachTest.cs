using Kafka.Streams;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Tests.Helpers;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamForeachTest
    {

        private readonly string topicName = "topic";
        private readonly ConsumerRecordFactory<int, string> recordFactory = new ConsumerRecordFactory<int, string>(Serdes.Int(), Serdes.String());
        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

        [Fact]
        public void testForeach()
        {
            // Given
            List<KeyValuePair<int, string>> inputRecords = new List<KeyValuePair<int, string>>
            {
                KeyValuePair.Create(0, "zero"),
                KeyValuePair.Create(1, "one"),
                KeyValuePair.Create(2, "two"),
                KeyValuePair.Create(3, "three"),
            };

            List<KeyValuePair<int, string>> expectedRecords = new List<KeyValuePair<int, string>>
            {
                KeyValuePair.Create(0, "TimeSpan.Zero"),
                KeyValuePair.Create(2, "ONE"),
                KeyValuePair.Create(4, "TWO"),
                KeyValuePair.Create(6, "THREE"),
            };

            var actualRecords = new List<KeyValuePair<int, string>>();
            void action(int key, string value) => actualRecords.Add(KeyValuePair.Create(key * 2, value.ToUpper()));

            // When
            var builder = new StreamsBuilder();
            IKStream<int, string> stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
            stream.ForEach(action);

            // Then
            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
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
            static void consume(int key, object value) { }

            new StreamsBuilder()
                .Stream<int, string>("emptyTopic")
                .ForEach(consume);
        }
    }
}
