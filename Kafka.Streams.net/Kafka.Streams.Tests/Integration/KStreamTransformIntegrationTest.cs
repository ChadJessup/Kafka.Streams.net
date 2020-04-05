//using Confluent.Kafka;
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.Integration
//{
//    public class KStreamTransformIntegrationTest
//    {
//        private StreamsBuilder builder;
//        private readonly string topic = "stream";
//        private readonly string stateStoreName = "myTransformState";
//        private List<KeyValuePair<int, int>> results = new ArrayList<>();
//        private Action<int, int> action = (key, value) => results.Add(KeyValuePair.Create(key, value));
//        private IKStream<int, int> stream;

//        public void Before()
//        {
//            builder = new StreamsBuilder();
//            IStoreBuilder<IKeyValueStore<int, int>> KeyValueStoreBuilder =
//                    Stores.KeyValueStoreBuilder(
//                        Stores.PersistentKeyValueStore(stateStoreName),
//                        Serdes.Int(),
//                        Serdes.Int());
//            builder.addStateStore(KeyValueStoreBuilder);
//            stream = builder.Stream(topic, Consumed.With(Serdes.Int(), Serdes.Int()));
//        }

//        private void VerifyResult(List<KeyValuePair<int, int>> expected)
//        {
//            ConsumerRecordFactory<int, int> recordFactory =
//                new ConsumerRecordFactory<int, int>(Serializers.Int32, Serializers.Int32);
//            StreamsConfig props = StreamsTestUtils.getStreamsConfig(Serdes.Int(), Serdes.Int());
//            TopologyTestDriver driver = new TopologyTestDriver(builder.Build(), props);
//            driver.PipeInput(recordFactory.Create(topic, Array.asList(KeyValuePair.Create(1, 1),
//                                                                      KeyValuePair.Create(2, 2),
//                                                                      KeyValuePair.Create(3, 3),
//                                                                      KeyValuePair.Create(2, 1),
//                                                                      KeyValuePair.Create(2, 3),
//                                                                      KeyValuePair.Create(1, 3))));
//            Assert.Equal(results, (expected));
//        }

//        [Xunit.Fact]
//        public void ShouldTransform()
//        {
//            //          stream
//            //              .transform(() => new Transformer<int, int, KeyValuePair<int, int>>()
//            //              {
//            //                  private IKeyValueStore<int, int> state;
//            //
//            //      public void Init(ProcessorContext context)
//            //      {
//            //          state = (IKeyValueStore<int, int>)context.getStateStore(stateStoreName);
//            //      }
//            //
//            //
//            //      public KeyValuePair<int, int> Transform(int key, int value)
//            //      {
//            //          state.putIfAbsent(key, 0);
//            //          int storedValue = state.Get(key);
//            //          KeyValuePair<int, int> result = KeyValuePair.Create(key + 1, value + storedValue++);
//            //          state.put(key, storedValue);
//            //          return result;
//            //      }
//            //
//            //
//            //      public void Close()
//            //      {
//            //      }
//            //  }, "myTransformState")
//            //          .ForEach(action);

//            List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>>
//            {
//                KeyValuePair.Create(2, 1),
//                KeyValuePair.Create(3, 2),
//                KeyValuePair.Create(4, 3),
//                KeyValuePair.Create(3, 2),
//                KeyValuePair.Create(3, 5),
//                KeyValuePair.Create(2, 4),
//            };
//            //verifyResult(expected);
//        }

//        [Xunit.Fact]
//        public void ShouldFlatTransform()
//        {
//            //            stream
//            //                .flatTransform(() => new Transformer<int, int, Iterable<KeyValuePair<int, int>>>()
//            //                {
//            //                        private IKeyValueStore<int, int> state;
//            //
//            //
//            //
//            //        public void Init(ProcessorContext context)
//            //        {
//            //            state = (IKeyValueStore<int, int>)context.getStateStore(stateStoreName);
//            //        }
//            //
//            //
//            //        public Iterable<KeyValuePair<int, int>> Transform(int key, int value)
//            //        {
//            //            List<KeyValuePair<int, int>> result = new ArrayList<>();
//            //            state.putIfAbsent(key, 0);
//            //            int storedValue = state.Get(key);
//            //            for (int i = 0; i < 3; i++)
//            //            {
//            //                result.Add(KeyValuePair.Create(key + i, value + storedValue++));
//            //            }
//            //            state.put(key, storedValue);
//            //            return result;
//            //        }
//            //
//            //
//            //        public void Close()
//            //        {
//            //        }
//            //    }, "myTransformState")
//            //            .ForEach(action);

//            List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>>
//            {
//                KeyValuePair.Create(1, 1),
//                KeyValuePair.Create(2, 2),
//                KeyValuePair.Create(3, 3),
//                KeyValuePair.Create(2, 2),
//                KeyValuePair.Create(3, 3),
//                KeyValuePair.Create(4, 4),
//                KeyValuePair.Create(3, 3),
//                KeyValuePair.Create(4, 4),
//                KeyValuePair.Create(5, 5),
//                KeyValuePair.Create(2, 4),
//                KeyValuePair.Create(3, 5),
//                KeyValuePair.Create(4, 6),
//                KeyValuePair.Create(2, 9),
//                KeyValuePair.Create(3, 10),
//                KeyValuePair.Create(4, 11),
//                KeyValuePair.Create(1, 6),
//                KeyValuePair.Create(2, 7),
//                KeyValuePair.Create(3, 8),
//            };
//            // verifyResult(expected);
//        }

//        [Xunit.Fact]
//        public void ShouldTransformValuesWithValueTransformerWithKey()
//        {
//            //          stream
//            //              .transformValues(() => new ValueTransformerWithKey<int, int, int>()
//            //              {
//            //                      private IKeyValueStore<int, int> state;
//            //
//            //
//            //      public void Init(ProcessorContext context)
//            //      {
//            //          state = (IKeyValueStore<int, int>)context.getStateStore("myTransformState");
//            //      }
//            //
//            //
//            //      public int Transform(int key, int value)
//            //      {
//            //          state.putIfAbsent(key, 0);
//            //          int storedValue = state.Get(key);
//            //          int result = value + storedValue++;
//            //          state.put(key, storedValue);
//            //          return result;
//            //      }
//            //
//            //
//            //      public void Close()
//            //      {
//            //      }
//            //  }, "myTransformState")
//            //          .ForEach(action);

//            List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>>
//            {
//                KeyValuePair.Create(1, 1),
//                KeyValuePair.Create(2, 2),
//                KeyValuePair.Create(3, 3),
//                KeyValuePair.Create(2, 2),
//                KeyValuePair.Create(2, 5),
//                KeyValuePair.Create(1, 4),
//            };
//            // verifyResult(expected);
//        }

//        [Xunit.Fact]
//        public void ShouldTransformValuesWithValueTransformerWithoutKey()
//        {
//            //            stream
//            //                .transformValues(() => new ValueTransformer<int, int>()
//            //                {
//            //                        private IKeyValueStore<int, int> state;
//            //
//            //
//            //        public void Init(ProcessorContext context)
//            //        {
//            //            state = (IKeyValueStore<int, int>)context.getStateStore("myTransformState");
//            //        }
//            //
//            //
//            //        public int Transform(int value)
//            //        {
//            //            state.putIfAbsent(value, 0);
//            //            int counter = state.Get(value);
//            //            state.put(value, ++counter);
//            //            return counter;
//            //        }
//            //
//            //
//            //        public void Close()
//            //        {
//            //        }
//            //    }, "myTransformState")
//            //            .ForEach(action);

//            List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>> {
//                KeyValuePair.Create(1, 1),
//                KeyValuePair.Create(2, 1),
//                KeyValuePair.Create(3, 1),
//                KeyValuePair.Create(2, 2),
//                KeyValuePair.Create(2, 2),
//                KeyValuePair.Create(1, 3),
//            };
//            // verifyResult(expected);
//        }

//        [Xunit.Fact]
//        public void ShouldFlatTransformValuesWithKey()
//        {
//            //            stream
//            //                .flatTransformValues(() => new ValueTransformerWithKey<int, int, Iterable<int>>()
//            //                {
//            //                        private IKeyValueStore<int, int> state;
//            //
//            //
//            //        public void Init(ProcessorContext context)
//            //        {
//            //            state = (IKeyValueStore<int, int>)context.getStateStore("myTransformState");
//            //        }
//            //
//            //
//            //        public Iterable<int> Transform(int key, int value)
//            //        {
//            //            List<int> result = new ArrayList<>();
//            //            state.putIfAbsent(key, 0);
//            //            int storedValue = state.Get(key);
//            //            for (int i = 0; i < 3; i++)
//            //            {
//            //                result.Add(value + storedValue++);
//            //            }
//            //            state.put(key, storedValue);
//            //            return result;
//            //        }
//            //
//            //
//            //        public void Close()
//            //        {
//            //        }
//            //    }, "myTransformState")
//            //            .ForEach(action);

//            List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>>
//            {
//                KeyValuePair.Create(1, 1),
//                KeyValuePair.Create(1, 2),
//                KeyValuePair.Create(1, 3),
//                KeyValuePair.Create(2, 2),
//                KeyValuePair.Create(2, 3),
//                KeyValuePair.Create(2, 4),
//                KeyValuePair.Create(3, 3),
//                KeyValuePair.Create(3, 4),
//                KeyValuePair.Create(3, 5),
//                KeyValuePair.Create(2, 4),
//                KeyValuePair.Create(2, 5),
//                KeyValuePair.Create(2, 6),
//                KeyValuePair.Create(2, 9),
//                KeyValuePair.Create(2, 10),
//                KeyValuePair.Create(2, 11),
//                KeyValuePair.Create(1, 6),
//                KeyValuePair.Create(1, 7),
//                KeyValuePair.Create(1, 8),
//            };
//            // verifyResult(expected);
//        }

//        [Xunit.Fact]
//        public void ShouldFlatTransformValuesWithValueTransformerWithoutKey()
//        {
//            //            stream
//            //                .flatTransformValues(() => new ValueTransformer<int, Iterable<int>>()
//            //                {
//            //                        private IKeyValueStore<int, int> state;
//            //
//            //
//            //        public void Init(ProcessorContext context)
//            //        {
//            //            state = (IKeyValueStore<int, int>)context.getStateStore("myTransformState");
//            //        }
//            //
//            //
//            //        public Iterable<int> Transform(int value)
//            //        {
//            //            List<int> result = new ArrayList<>();
//            //            state.putIfAbsent(value, 0);
//            //            int counter = state.Get(value);
//            //            for (int i = 0; i < 3; i++)
//            //            {
//            //                result.Add(++counter);
//            //            }
//            //            state.put(value, counter);
//            //            return result;
//            //        }
//            //
//            //
//            //        public void Close()
//            //        {
//            //        }
//            //    }, "myTransformState")
//            //            .ForEach(action);

//            List<KeyValuePair<int, int>> expected = new List<KeyValuePair<int, int>>
//            {
//                KeyValuePair.Create(1, 1),
//                KeyValuePair.Create(1, 2),
//                KeyValuePair.Create(1, 3),
//                KeyValuePair.Create(2, 1),
//                KeyValuePair.Create(2, 2),
//                KeyValuePair.Create(2, 3),
//                KeyValuePair.Create(3, 1),
//                KeyValuePair.Create(3, 2),
//                KeyValuePair.Create(3, 3),
//                KeyValuePair.Create(2, 4),
//                KeyValuePair.Create(2, 5),
//                KeyValuePair.Create(2, 6),
//                KeyValuePair.Create(2, 4),
//                KeyValuePair.Create(2, 5),
//                KeyValuePair.Create(2, 6),
//                KeyValuePair.Create(1, 7),
//                KeyValuePair.Create(1, 8),
//                KeyValuePair.Create(1, 9),
//            };
//            // verifyResult(expected);
//        }
//    }
//}
