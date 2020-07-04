using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.KStream;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.Temporary;
using Kafka.Streams.Tests.Helpers;
using System;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Integration
{
    public class KStreamTransformIntegrationTest
    {
        private readonly StreamsBuilder builder;
        private readonly string topic = "stream";
        private readonly string stateStoreName = "myTransformState";
        private readonly List<KeyValuePair<int, int>> results = new List<KeyValuePair<int, int>>();
        private readonly Action<int, int> action;
        private readonly IKStream<int, int> stream;

        public KStreamTransformIntegrationTest()
        {
            action = (key, value) => results.Add(KeyValuePair.Create(key, value));
            builder = new StreamsBuilder();
            // IStoreBuilder<IKeyValueStore<int, int>> KeyValueStoreBuilder =
            //         Stores.KeyValueStoreBuilder(
            //             Stores.PersistentKeyValueStore(stateStoreName),
            //             Serdes.Int(),
            //             Serdes.Int());
            // builder.AddStateStore(KeyValueStoreBuilder);
            stream = builder.Stream(topic, Consumed.With(Serdes.Int(), Serdes.Int()));
        }

        private void VerifyResult(List<KeyValuePair<int, int>> expected)
        {
            ConsumerRecordFactory<int, int> recordFactory =
                new ConsumerRecordFactory<int, int>(Serializers.Int32, Serializers.Int32);

            StreamsConfig props = StreamsTestConfigs.GetStandardConfig(
                Serdes.Int(),
                Serdes.Int());

            TopologyTestDriver driver = new TopologyTestDriver(builder.Context, builder.Build(), props);
            driver.PipeInput(recordFactory.Create(
                topic,
                Arrays.asList(
                    KeyValuePair.Create(1, 1),
                    KeyValuePair.Create(2, 2),
                    KeyValuePair.Create(3, 3),
                    KeyValuePair.Create(2, 1),
                    KeyValuePair.Create(2, 3),
                    KeyValuePair.Create(1, 3))));
            Assert.Equal(results, expected);
        }

        [Fact]
        public void ShouldTransform()
        {
            //          stream
            //              .transform(() => new Transformer<int, int, KeyValuePair<int, int>>()
            //              {
            //                  private IKeyValueStore<int, int> state;
            //
            //      public void Init(IProcessorContext context)
            //      {
            //          state = (IKeyValueStore<int, int>)context.getStateStore(stateStoreName);
            //      }
            //
            //
            //      public KeyValuePair<int, int> Transform(int key, int value)
            //      {
            //          state.PutIfAbsent(key, 0);
            //          int storedValue = state.Get(key);
            //          KeyValuePair<int, int> result = KeyValuePair.Create(key + 1, value + storedValue++);
            //          state.Put(key, storedValue);
            //          return result;
            //      }
            //
            //
            //      public void Close()
            //      {
            //      }
            //  }, "myTransformState")
            //          .ForEach(action);

            List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>>
            {
                KeyValuePair.Create(2, 1),
                KeyValuePair.Create(3, 2),
                KeyValuePair.Create(4, 3),
                KeyValuePair.Create(3, 2),
                KeyValuePair.Create(3, 5),
                KeyValuePair.Create(2, 4),
            };
            //verifyResult(expected);
        }

        [Fact]
        public void ShouldFlatTransform()
        {
            //            stream
            //                .flatTransform(() => new Transformer<int, int, Iterable<KeyValuePair<int, int>>>()
            //                {
            //                        private IKeyValueStore<int, int> state;
            //
            //
            //
            //        public void Init(IProcessorContext context)
            //        {
            //            state = (IKeyValueStore<int, int>)context.getStateStore(stateStoreName);
            //        }
            //
            //
            //        public Iterable<KeyValuePair<int, int>> Transform(int key, int value)
            //        {
            //            List<KeyValuePair<int, int>> result = new List<KeyValuePair<int, int>>();
            //            state.PutIfAbsent(key, 0);
            //            int storedValue = state.Get(key);
            //            for (int i = 0; i < 3; i++)
            //            {
            //                result.Add(KeyValuePair.Create(key + i, value + storedValue++));
            //            }
            //            state.Put(key, storedValue);
            //            return result;
            //        }
            //
            //
            //        public void Close()
            //        {
            //        }
            //    }, "myTransformState")
            //            .ForEach(action);

            List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>>
            {
                KeyValuePair.Create(1, 1),
                KeyValuePair.Create(2, 2),
                KeyValuePair.Create(3, 3),
                KeyValuePair.Create(2, 2),
                KeyValuePair.Create(3, 3),
                KeyValuePair.Create(4, 4),
                KeyValuePair.Create(3, 3),
                KeyValuePair.Create(4, 4),
                KeyValuePair.Create(5, 5),
                KeyValuePair.Create(2, 4),
                KeyValuePair.Create(3, 5),
                KeyValuePair.Create(4, 6),
                KeyValuePair.Create(2, 9),
                KeyValuePair.Create(3, 10),
                KeyValuePair.Create(4, 11),
                KeyValuePair.Create(1, 6),
                KeyValuePair.Create(2, 7),
                KeyValuePair.Create(3, 8),
            };
            // verifyResult(expected);
        }

        [Fact]
        public void ShouldTransformValuesWithValueTransformerWithKey()
        {
            //          stream
            //              .transformValues(() => new ValueTransformerWithKey<int, int, int>()
            //              {
            //                      private IKeyValueStore<int, int> state;
            //
            //
            //      public void Init(IProcessorContext context)
            //      {
            //          state = (IKeyValueStore<int, int>)context.getStateStore("myTransformState");
            //      }
            //
            //
            //      public int Transform(int key, int value)
            //      {
            //          state.PutIfAbsent(key, 0);
            //          int storedValue = state.Get(key);
            //          int result = value + storedValue++;
            //          state.Put(key, storedValue);
            //          return result;
            //      }
            //
            //
            //      public void Close()
            //      {
            //      }
            //  }, "myTransformState")
            //          .ForEach(action);

            List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>>
            {
                KeyValuePair.Create(1, 1),
                KeyValuePair.Create(2, 2),
                KeyValuePair.Create(3, 3),
                KeyValuePair.Create(2, 2),
                KeyValuePair.Create(2, 5),
                KeyValuePair.Create(1, 4),
            };
            // verifyResult(expected);
        }

        [Fact]
        public void ShouldTransformValuesWithValueTransformerWithoutKey()
        {
            //            stream
            //                .transformValues(() => new ValueTransformer<int, int>()
            //                {
            //                        private IKeyValueStore<int, int> state;
            //
            //
            //        public void Init(IProcessorContext context)
            //        {
            //            state = (IKeyValueStore<int, int>)context.getStateStore("myTransformState");
            //        }
            //
            //
            //        public int Transform(int value)
            //        {
            //            state.PutIfAbsent(value, 0);
            //            int counter = state.Get(value);
            //            state.Put(value, ++counter);
            //            return counter;
            //        }
            //
            //
            //        public void Close()
            //        {
            //        }
            //    }, "myTransformState")
            //            .ForEach(action);

            List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>> {
                KeyValuePair.Create(1, 1),
                KeyValuePair.Create(2, 1),
                KeyValuePair.Create(3, 1),
                KeyValuePair.Create(2, 2),
                KeyValuePair.Create(2, 2),
                KeyValuePair.Create(1, 3),
            };
            // verifyResult(expected);
        }

        [Fact]
        public void ShouldFlatTransformValuesWithKey()
        {
            //            stream
            //                .flatTransformValues(() => new ValueTransformerWithKey<int, int, Iterable<int>>()
            //                {
            //                        private IKeyValueStore<int, int> state;
            //
            //
            //        public void Init(IProcessorContext context)
            //        {
            //            state = (IKeyValueStore<int, int>)context.getStateStore("myTransformState");
            //        }
            //
            //
            //        public Iterable<int> Transform(int key, int value)
            //        {
            //            List<int> result = new List<int>();
            //            state.PutIfAbsent(key, 0);
            //            int storedValue = state.Get(key);
            //            for (int i = 0; i < 3; i++)
            //            {
            //                result.Add(value + storedValue++);
            //            }
            //            state.Put(key, storedValue);
            //            return result;
            //        }
            //
            //
            //        public void Close()
            //        {
            //        }
            //    }, "myTransformState")
            //            .ForEach(action);

            List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>>
            {
                KeyValuePair.Create(1, 1),
                KeyValuePair.Create(1, 2),
                KeyValuePair.Create(1, 3),
                KeyValuePair.Create(2, 2),
                KeyValuePair.Create(2, 3),
                KeyValuePair.Create(2, 4),
                KeyValuePair.Create(3, 3),
                KeyValuePair.Create(3, 4),
                KeyValuePair.Create(3, 5),
                KeyValuePair.Create(2, 4),
                KeyValuePair.Create(2, 5),
                KeyValuePair.Create(2, 6),
                KeyValuePair.Create(2, 9),
                KeyValuePair.Create(2, 10),
                KeyValuePair.Create(2, 11),
                KeyValuePair.Create(1, 6),
                KeyValuePair.Create(1, 7),
                KeyValuePair.Create(1, 8),
            };
            // verifyResult(expected);
        }

        [Fact]
        public void ShouldFlatTransformValuesWithValueTransformerWithoutKey()
        {
            //            stream
            //                .flatTransformValues(() => new ValueTransformer<int, Iterable<int>>()
            //                {
            //                        private IKeyValueStore<int, int> state;
            //
            //
            //        public void Init(IProcessorContext context)
            //        {
            //            state = (IKeyValueStore<int, int>)context.getStateStore("myTransformState");
            //        }
            //
            //
            //        public Iterable<int> Transform(int value)
            //        {
            //            List<int> result = new List<int>();
            //            state.PutIfAbsent(value, 0);
            //            int counter = state.Get(value);
            //            for (int i = 0; i < 3; i++)
            //            {
            //                result.Add(++counter);
            //            }
            //            state.Put(value, counter);
            //            return result;
            //        }
            //
            //
            //        public void Close()
            //        {
            //        }
            //    }, "myTransformState")
            //            .ForEach(action);

            List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>>
            {
                KeyValuePair.Create(1, 1),
                KeyValuePair.Create(1, 2),
                KeyValuePair.Create(1, 3),
                KeyValuePair.Create(2, 1),
                KeyValuePair.Create(2, 2),
                KeyValuePair.Create(2, 3),
                KeyValuePair.Create(3, 1),
                KeyValuePair.Create(3, 2),
                KeyValuePair.Create(3, 3),
                KeyValuePair.Create(2, 4),
                KeyValuePair.Create(2, 5),
                KeyValuePair.Create(2, 6),
                KeyValuePair.Create(2, 4),
                KeyValuePair.Create(2, 5),
                KeyValuePair.Create(2, 6),
                KeyValuePair.Create(1, 7),
                KeyValuePair.Create(1, 8),
                KeyValuePair.Create(1, 9),
            };
            // verifyResult(expected);
        }
    }
}
