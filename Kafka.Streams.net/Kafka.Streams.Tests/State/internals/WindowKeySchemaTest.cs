//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */


























//    public class WindowKeySchemaTest
//    {

//        private readonly string key = "key";
//        private readonly string topic = "topic";
//        private readonly long startTime = 50L;
//        private readonly long endTime = 100L;
//        private ISerde<string> serde = Serdes.String();

//        private Window window = new TimeWindow(startTime, endTime);
//        private Windowed<string> windowedKey = new Windowed<>(key, window);
//        private WindowKeySchema windowKeySchema = new WindowKeySchema();
//        private ISerde<Windowed<string>> keySerde = new WindowedSerdes.TimeWindowedSerde<>(serde);
//        private StateSerdes<string, byte[]> stateSerdes = new StateSerdes<string, byte[]>("dummy", serde, Serdes.ByteArray());

//        [Fact]
//        public void TestHasNextConditionUsingNullKeys()
//        {
//            List<KeyValuePair<Bytes, int>> keys = Array.asList(
//                    KeyValuePair.Create(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0, 0 }), new TimeWindow(0, 1)), 0), 1),
//                    KeyValuePair.Create(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0 }), new TimeWindow(0, 1)), 0), 2),
//                    KeyValuePair.Create(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0, 0, 0 }), new TimeWindow(0, 1)), 0), 3),
//                    KeyValuePair.Create(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0 }), new TimeWindow(10, 20)), 4), 4),
//                    KeyValuePair.Create(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0, 0 }), new TimeWindow(10, 20)), 5), 5),
//                    KeyValuePair.Create(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0, 0, 0 }), new TimeWindow(10, 20)), 6), 6));
//            DelegatingPeekingKeyValueIterator<Bytes, int> iterator = new DelegatingPeekingKeyValueIterator<>("foo", new KeyValueIteratorStub<>(keys.iterator()));

//            HasNextCondition hasNextCondition = windowKeySchema.hasNextCondition(null, null, 0, long.MaxValue);
//            List<int> results = new ArrayList<>();
//            while (hasNextCondition.hasNext(iterator))
//            {
//                results.Add(iterator.MoveNext().value);
//            }

//            Assert.Equal(results, Array.asList(1, 2, 3, 4, 5, 6));
//        }

//        [Fact]
//        public void TestUpperBoundWithLargeTimestamps()
//        {
//            Bytes upper = windowKeySchema.upperRange(Bytes.Wrap(new byte[] { 0xA, 0xB, 0xC }), long.MaxValue);

//            Assert.Equal(
//                "shorter key with max timestamp should be in range",
//                upper.compareTo(
//                    WindowKeySchema.toStoreKeyBinary(
//                        new byte[] { 0xA },
//                        long.MaxValue,
//                        int.MaxValue
//                    )
//                ) >= 0
//            );

//            Assert.Equal(
//                "shorter key with max timestamp should be in range",
//                upper.compareTo(
//                    WindowKeySchema.toStoreKeyBinary(
//                        new byte[] { 0xA, 0xB },
//                        long.MaxValue,
//                        int.MaxValue
//                    )
//                ) >= 0
//            );

//            Assert.Equal(upper, WindowKeySchema.toStoreKeyBinary(new byte[] { 0xA }, long.MaxValue, int.MaxValue));
//        }

//        [Fact]
//        public void TestUpperBoundWithKeyBytesLargerThanFirstTimestampByte()
//        {
//            Bytes upper = windowKeySchema.upperRange(Bytes.Wrap(new byte[] { 0xA, (byte)0x8F, (byte)0x9F }), long.MaxValue);

//            Assert.Equal(
//                "shorter key with max timestamp should be in range",
//                upper.compareTo(
//                    WindowKeySchema.toStoreKeyBinary(
//                        new byte[] { 0xA, (byte)0x8F },
//                        long.MaxValue,
//                        int.MaxValue
//                    )
//                ) >= 0
//            );

//            Assert.Equal(upper, WindowKeySchema.toStoreKeyBinary(new byte[] { 0xA, (byte)0x8F, (byte)0x9F }, long.MaxValue, int.MaxValue));
//        }


//        [Fact]
//        public void TestUpperBoundWithKeyBytesLargerAndSmallerThanFirstTimestampByte()
//        {
//            Bytes upper = windowKeySchema.upperRange(Bytes.Wrap(new byte[] { 0xC, 0xC, 0x9 }), 0x0AffffffffffffffL);

//            Assert.Equal(
//                "shorter key with max timestamp should be in range",
//                upper.compareTo(
//                    WindowKeySchema.toStoreKeyBinary(
//                        new byte[] { 0xC, 0xC },
//                        0x0AffffffffffffffL,
//                        int.MaxValue
//                    )
//                ) >= 0
//            );

//            Assert.Equal(upper, WindowKeySchema.toStoreKeyBinary(new byte[] { 0xC, 0xC }, 0x0AffffffffffffffL, int.MaxValue));
//        }

//        [Fact]
//        public void TestUpperBoundWithZeroTimestamp()
//        {
//            Bytes upper = windowKeySchema.upperRange(Bytes.Wrap(new byte[] { 0xA, 0xB, 0xC }), 0);
//            Assert.Equal(upper, WindowKeySchema.toStoreKeyBinary(new byte[] { 0xA, 0xB, 0xC }, 0, int.MaxValue));
//        }

//        [Fact]
//        public void TestLowerBoundWithZeroTimestamp()
//        {
//            Bytes lower = windowKeySchema.lowerRange(Bytes.Wrap(new byte[] { 0xA, 0xB, 0xC }), 0);
//            Assert.Equal(lower, WindowKeySchema.toStoreKeyBinary(new byte[] { 0xA, 0xB, 0xC }, 0, 0));
//        }

//        [Fact]
//        public void TestLowerBoundWithMonZeroTimestamp()
//        {
//            Bytes lower = windowKeySchema.lowerRange(Bytes.Wrap(new byte[] { 0xA, 0xB, 0xC }), 42);
//            Assert.Equal(lower, WindowKeySchema.toStoreKeyBinary(new byte[] { 0xA, 0xB, 0xC }, 0, 0));
//        }

//        [Fact]
//        public void TestLowerBoundMatchesTrailingZeros()
//        {
//            Bytes lower = windowKeySchema.lowerRange(Bytes.Wrap(new byte[] { 0xA, 0xB, 0xC }), long.MaxValue - 1);

//            Assert.Equal(
//                "appending zeros to key should still be in range",
//                lower.compareTo(
//                        WindowKeySchema.toStoreKeyBinary(
//                            new byte[] { 0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
//                            long.MaxValue - 1,
//                            0
//                    )
//                ) < 0
//            );

//            Assert.Equal(lower, WindowKeySchema.toStoreKeyBinary(new byte[] { 0xA, 0xB, 0xC }, 0, 0));
//        }

//        [Fact]
//        public void ShouldSerializeDeserialize()
//        {
//            byte[] bytes = keySerde.Serializer.Serialize(topic, windowedKey);
//            Windowed<string> result = keySerde.Deserializer.Deserialize(topic, bytes);
//            // TODO: fix this part as last bits of KAFKA-4468
//            Assert.Equal(new Windowed<>(key, new TimeWindow(startTime, long.MaxValue)), result);
//        }

//        [Fact]
//        public void TestSerializeDeserializeOverflowWindowSize()
//        {
//            byte[] bytes = keySerde.Serializer.Serialize(topic, windowedKey);
//            Windowed<string> result = new TimeWindowedDeserializer<>(serde.deserializer(), long.MaxValue - 1)
//                    .deserialize(topic, bytes);
//            Assert.Equal(new Windowed<>(key, new TimeWindow(startTime, long.MaxValue)), result);
//        }

//        [Fact]
//        public void ShouldSerializeDeserializeExpectedWindowSize()
//        {
//            byte[] bytes = keySerde.Serializer.Serialize(topic, windowedKey);
//            Windowed<string> result = new TimeWindowedDeserializer<>(serde.deserializer(), endTime - startTime)
//                .deserialize(topic, bytes);
//            Assert.Equal(windowedKey, result);
//        }

//        [Fact]
//        public void ShouldSerializeDeserializeExpectedChangelogWindowSize()
//        {
//            // Key-value containing serialized store key binary and the key's window size
//            List<KeyValuePair<Bytes, int>> keys = Array.asList(
//                KeyValuePair.Create(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0 }), new TimeWindow(0, 1)), 0), 1),
//                KeyValuePair.Create(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0, 0 }), new TimeWindow(0, 10)), 0), 10),
//                KeyValuePair.Create(WindowKeySchema.toStoreKeyBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0, 0, 0 }), new TimeWindow(10, 30)), 6), 20));

//            List<long> results = new ArrayList<>();
//            foreach (KeyValuePair<Bytes, int> keyValue in keys)
//            {
//                // Let the deserializer know that it's deserializing a changelog windowed key
//                Serde<Windowed<string>> keySerde = new WindowedSerdes.TimeWindowedSerde<>(serde, keyValue.value).forChangelog(true);
//                Windowed<string> result = keySerde.deserializer().deserialize(topic, keyValue.key.Get());
//                Window resultWindow = result.window();
//                results.Add(resultWindow.end() - resultWindow.start());
//            }

//            Assert.Equal(results, Array.asList(1L, 10L, 20L));
//        }

//        [Fact]
//        public void ShouldSerializeNullToNull()
//        {
//            Assert.Null(keySerde.Serializer.Serialize(topic, null));
//        }

//        [Fact]
//        public void ShouldDeserializeEmptyByteArrayToNull()
//        {
//            Assert.Null(keySerde.Deserializer.Deserialize(topic, System.Array.Empty<byte>()));
//        }

//        [Fact]
//        public void ShouldDeserializeNullToNull()
//        {
//            Assert.Null(keySerde.Deserializer.Deserialize(topic, null));
//        }

//        [Fact]
//        public void ShouldConvertToBinaryAndBack()
//        {
//            Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
//            Windowed<string> result = WindowKeySchema.fromStoreKey(serialized.Get(), endTime - startTime, stateSerdes.keyDeserializer(), stateSerdes.Topic);
//            Assert.Equal(windowedKey, result);
//        }

//        [Fact]
//        public void ShouldExtractEndTimeFromBinary()
//        {
//            Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
//            Assert.Equal(0, WindowKeySchema.extractStoreSequence(serialized.Get()));
//        }

//        [Fact]
//        public void ShouldExtractStartTimeFromBinary()
//        {
//            Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
//            Assert.Equal(startTime, WindowKeySchema.extractStoreTimestamp(serialized.Get()));
//        }

//        [Fact]
//        public void ShouldExtractWindowFromBinary()
//        {
//            Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
//            Assert.Equal(window, WindowKeySchema.extractStoreWindow(serialized.Get(), endTime - startTime));
//        }

//        [Fact]
//        public void ShouldExtractKeyBytesFromBinary()
//        {
//            Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
//            Array.Equals(key.getBytes(), WindowKeySchema.extractStoreKeyBytes(serialized.Get()));
//        }

//        [Fact]
//        public void ShouldExtractKeyFromBinary()
//        {
//            Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedKey, 0, stateSerdes);
//            Assert.Equal(windowedKey, WindowKeySchema.fromStoreKey(serialized.Get(), endTime - startTime, stateSerdes.keyDeserializer(), stateSerdes.Topic));
//        }

//        [Fact]
//        public void ShouldExtractBytesKeyFromBinary()
//        {
//            Windowed<Bytes> windowedBytesKey = new Windowed<Bytes>(Bytes.Wrap(key.getBytes()), window);
//            Bytes serialized = WindowKeySchema.toStoreKeyBinary(windowedBytesKey, 0);
//            Assert.Equal(windowedBytesKey, WindowKeySchema.fromStoreBytesKey(serialized.Get(), endTime - startTime));
//        }
//    }
//}
///*






//*

//*





//*/


























