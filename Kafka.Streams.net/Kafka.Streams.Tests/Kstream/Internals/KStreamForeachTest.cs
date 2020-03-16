//using Kafka.Streams;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Tests.Helpers;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class KStreamForeachTest
//    {

//        private string topicName = "topic";
//        private ConsumerRecordFactory<int, string> recordFactory = new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String());
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

//        [Fact]
//        public void testForeach()
//        {
//            // Given
//            List<KeyValue<int, string>> inputRecords = Array.AsReadOnly(
//                new KeyValue<>(0, "zero"),
//                new KeyValue<>(1, "one"),
//                new KeyValue<>(2, "two"),
//                new KeyValue<>(3, "three")
//            );

//            List<KeyValue<int, string>> expectedRecords = Array.AsReadOnly(
//                new KeyValue<>(0, "ZERO"),
//                new KeyValue<>(2, "ONE"),
//                new KeyValue<>(4, "TWO"),
//                new KeyValue<>(6, "THREE")
//            );

//            var actualRecords = new List<KeyValue<int, string>>();
//            ForeachAction<int, string> action =
//                (key, value) => actualRecords.add(new KeyValue<>(key * 2, value.toUppercase(Locale.ROOT)));

//            // When
//            var builder = new StreamsBuilder();
//            IKStream<int, string> stream = builder.Stream(topicName, Consumed.with(Serdes.Int(), Serdes.String()));
//            stream.ForEach(action);

//            // Then
//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (KeyValue<int, string> record in inputRecords)
//            {
//                driver.pipeInput(recordFactory.create(topicName, record.key, record.value));
//            }

//            Assert.Equal(expectedRecords.Count, actualRecords.Count);
//            for (var i = 0; i < expectedRecords.Count; i++)
//            {
//                KeyValue<int, string> expectedRecord = expectedRecords[i];
//                KeyValue<int, string> actualRecord = actualRecords[i];
//                Assert.Equal(expectedRecord, actualRecord);
//            }
//        }

//        [Fact]
//        public void testTypeVariance()
//        {
//            ForeachAction<int, object> consume = (key, value) => { };

//            new StreamsBuilder()
//                .Stream<int, string>("emptyTopic")
//                .ForEach(consume);
//        }
//    }
//}
