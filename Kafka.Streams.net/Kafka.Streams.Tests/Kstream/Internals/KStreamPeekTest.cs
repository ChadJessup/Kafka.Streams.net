using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.Tests.Kstream.Internals
{
    public class KStreamPeekTest
    {
        private string topicName = "topic";
        private ConsumerRecordFactory<int, string> recordFactory = new ConsumerRecordFactory<>(Serdes.Int(), Serdes.String());
        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

        [Fact]
        public void shouldObserveStreamElements()
        {
            var builder = new StreamsBuilder();
            IKStream<K, V> stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
            List<KeyValuePair<int, string>> peekObserved = new List<>(), streamObserved = new List<>();
            stream.peek(collect(peekObserved)).ForEach(collect(streamObserved));

            var driver = new TopologyTestDriver(builder.Build(), props);
            List<KeyValuePair<int, string>> expected = new List<>();
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
            IKStream<K, V> stream = builder.Stream(topicName, Consumed.With(Serdes.Int(), Serdes.String()));
            try
            {
                stream.peek(null);
                Assert.False(true, "expected null action to throw NPE");
            }
            catch (NullReferenceException expected)
            {
                // do nothing
            }
        }

        private static ForeachAction<K, V> collect<K, V>(List<KeyValuePair<K, V>> into)
        {
            return (key, value) => into.Add(KeyValuePair.Create(key, value));
        }
    }
}
