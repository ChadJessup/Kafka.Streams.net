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
    public class KStreamPeekTest
    {
        private readonly string topicName = "topic";
        private readonly ConsumerRecordFactory<int, string> recordFactory = new ConsumerRecordFactory<int, string>(Serdes.Int(), Serdes.String());
        private readonly StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

        [Fact]
        public void shouldObserveStreamElements()
        {
            var builder = new StreamsBuilder();
            IKStream<int, string> stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
            List<KeyValuePair<int, string>> peekObserved = new List<KeyValuePair<int, string>>(), streamObserved = new List<KeyValuePair<int, string>>();
            stream.Peek(collect(peekObserved)).ForEach(collect(streamObserved));

            var driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            List<KeyValuePair<int, string>> expected = new List<KeyValuePair<int, string>>();
            for (var key = 0; key < 32; key++)
            {
                var value = "V" + key;
                driver.PipeInput(recordFactory.Create(topicName, key, value));
                expected.Add(KeyValuePair.Create(key, value));
            }

            Assert.Equal(expected, peekObserved);
            Assert.Equal(expected, streamObserved);
        }

        [Fact]
        public void shouldNotAllowNullAction()
        {
            var builder = new StreamsBuilder();
            IKStream<int, string> stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
            Assert.Throws<ArgumentNullException>(() => stream.Peek(null));
        }

        private static Action<K, V> collect<K, V>(List<KeyValuePair<K, V>> into)
        {
            return (key, value) => into.Add(KeyValuePair.Create(key, value));
        }
    }
}
