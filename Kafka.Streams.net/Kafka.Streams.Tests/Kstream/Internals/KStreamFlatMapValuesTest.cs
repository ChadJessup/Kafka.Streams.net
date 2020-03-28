///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//using Kafka.Streams.Configs;
//using Kafka.Streams.Kafka.Streams;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.Tests.Helpers;
//using Xunit;

//namespace Kafka.Streams.KStream.Internals
//{




















//    public class KStreamFlatMapValuesTest
//    {
//        private string topicName = "topic";
//        private ConsumerRecordFactory<int, int> recordFactory =
//            new ConsumerRecordFactory<>(Serdes.Int(), Serdes.Int(), 0L);
//        private StreamsConfig props = StreamsTestConfigs.GetStandardConfig(Serdes.Int(), Serdes.String());

//        [Fact]
//        public void testFlatMapValues()
//        {
//            var builder = new StreamsBuilder();

//            IValueMapper<int, Iterable<string>> mapper =
//                value =>
//                {
//                    List<string> result = new List<>();
//                    result.add("v" + value);
//                    result.add("V" + value);
//                    return result;
//                };

//            int[] expectedKeys = { 0, 1, 2, 3 };

//            IKStream<int, int> stream = builder.Stream(topicName, Consumed.with(Serdes.Int(), Serdes.Int()));
//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();
//            stream.flatMapValues(mapper).process(supplier);

//            try
//            {
//                var driver = new TopologyTestDriver(builder.Build(), props);
//                foreach (var expectedKey in expectedKeys)
//                {
//                    // .Assing the timestamp to recordFactory.create to disambiguate the call
//                    driver.pipeInput(recordFactory.create(topicName, expectedKey, expectedKey, 0L));
//                }
//            }

//        KeyValueTimestamp[] expected = {new KeyValueTimestamp<>(0, "v0", 0), new KeyValueTimestamp<>(0, "V0", 0),
//            new KeyValueTimestamp<>(1, "v1", 0), new KeyValueTimestamp<>(1, "V1", 0),
//            new KeyValueTimestamp<>(2, "v2", 0), new KeyValueTimestamp<>(2, "V2", 0),
//            new KeyValueTimestamp<>(3, "v3", 0), new KeyValueTimestamp<>(3, "V3", 0)};

//            Assert.Equal(expected, supplier.theCapturedProcessor().processed.ToArray());
//        }


//        [Fact]
//        public void testFlatMapValuesWithKeys()
//        {
//            var builder = new StreamsBuilder();

//            ValueMapperWithKey<int, int, Iterable<string>> mapper =
//                (readOnlyKey, value) =>
//                {
//                    List<string> result = new List<>();
//                    result.add("v" + value);
//                    result.add("k" + readOnlyKey);
//                    return result;
//                };

//            int[] expectedKeys = { 0, 1, 2, 3 };

//            IKStream<int, int> stream = builder.Stream(topicName, Consumed.with(Serdes.Int(), Serdes.Int()));
//            MockProcessorSupplier<int, string> supplier = new MockProcessorSupplier<>();

//            stream.flatMapValues(mapper).process(supplier);


//            var driver = new TopologyTestDriver(builder.Build(), props);
//            foreach (var expectedKey in expectedKeys)
//            {
//                // .Assing the timestamp to recordFactory.create to disambiguate the call
//                driver.pipeInput(recordFactory.create(topicName, expectedKey, expectedKey, 0L));
//            }
//        }

//        KeyValueTimestamp[] expected = {new KeyValueTimestamp<>(0, "v0", 0),
//            new KeyValueTimestamp<>(0, "k0", 0),
//            new KeyValueTimestamp<>(1, "v1", 0),
//            new KeyValueTimestamp<>(1, "k1", 0),
//            new KeyValueTimestamp<>(2, "v2", 0),
//            new KeyValueTimestamp<>(2, "k2", 0),
//            new KeyValueTimestamp<>(3, "v3", 0),
//            new KeyValueTimestamp<>(3, "k3", 0)};

//        Assert.Equal(expected, supplier.theCapturedProcessor().processed.ToArray());
//    }
//}