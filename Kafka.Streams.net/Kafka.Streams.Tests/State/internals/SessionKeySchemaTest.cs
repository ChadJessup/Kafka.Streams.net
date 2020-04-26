//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

























//    public class SessionKeySchemaTest
//    {

//        private readonly string key = "key";
//        private readonly string topic = "topic";
//        private readonly long startTime = 50L;
//        private readonly long endTime = 100L;
//        private ISerde<string> serde = Serdes.String();

//        private Window window = new SessionWindow(startTime, endTime);
//        private IWindowed<string> windowedKey = new Windowed<string>(key, window);
//        private Serde<IWindowed<string>> keySerde = new WindowedSerdes.SessionWindowedSerde<string>(serde);

//        private SessionKeySchema sessionKeySchema = new SessionKeySchema();
//        private DelegatingPeekingKeyValueIterator<Bytes, int> iterator;


//        public void Before()
//        {
//            List<KeyValuePair<Bytes, int>> keys = Arrays.asList(
//                KeyValuePair.Create(SessionKeySchema.toBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0, 0 }), new SessionWindow(0, 0))), 1),
//                KeyValuePair.Create(SessionKeySchema.toBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0 }), new SessionWindow(0, 0))), 2),
//                KeyValuePair.Create(SessionKeySchema.toBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0, 0, 0 }), new SessionWindow(0, 0))), 3),
//                KeyValuePair.Create(SessionKeySchema.toBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0 }), new SessionWindow(10, 20))), 4),
//                KeyValuePair.Create(SessionKeySchema.toBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0, 0 }), new SessionWindow(10, 20))), 5),
//                KeyValuePair.Create(SessionKeySchema.toBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0, 0, 0 }), new SessionWindow(10, 20))), 6));
            
//            iterator = new DelegatingPeekingKeyValueIterator<>("foo", new KeyValueIteratorStub<>(keys.iterator()));
//        }

//        [Fact]
//        public void ShouldFetchExactKeysSkippingLongerKeys()
//        {
//            Bytes key = Bytes.Wrap(new byte[] { 0 });
//            List<int> result = getValues(sessionKeySchema.hasNextCondition(key, key, 0, long.MaxValue));
//            Assert.Equal(result, Arrays.asList(2, 4));
//        }

//        [Fact]
//        public void ShouldFetchExactKeySkippingShorterKeys()
//        {
//            Bytes key = Bytes.Wrap(new byte[] { 0, 0 });
//            HasNextCondition hasNextCondition = sessionKeySchema.hasNextCondition(key, key, 0, long.MaxValue);
//            List<int> results = GetValues(hasNextCondition);
//            Assert.Equal(results, Arrays.asList(1, 5));
//        }

//        [Fact]
//        public void ShouldFetchAllKeysUsingNullKeys()
//        {
//            HasNextCondition hasNextCondition = sessionKeySchema.hasNextCondition(null, null, 0, long.MaxValue);
//            List<int> results = GetValues(hasNextCondition);
//            Assert.Equal(results, Arrays.asList(1, 2, 3, 4, 5, 6));
//        }

//        [Fact]
//        public void TestUpperBoundWithLargeTimestamps()
//        {
//            Bytes upper = sessionKeySchema.upperRange(Bytes.Wrap(new byte[] { 0xA, 0xB, 0xC }), long.MaxValue);

//            Assert.Equal(
//                "shorter key with max timestamp should be in range",
//                upper.CompareTo(SessionKeySchema.toBinary(
//                    new Windowed<>(
//                        Bytes.Wrap(new byte[] { 0xA }),
//                        new SessionWindow(long.MaxValue, long.MaxValue))
//                )) >= 0
//            );

//            Assert.Equal(
//                "shorter key with max timestamp should be in range",
//                upper.CompareTo(SessionKeySchema.toBinary(
//                    new Windowed<>(
//                        Bytes.Wrap(new byte[] { 0xA, 0xB }),
//                        new SessionWindow(long.MaxValue, long.MaxValue))

//                )) >= 0
//            );

//            Assert.Equal(upper, SessionKeySchema.toBinary(
//                new Windowed<>(Bytes.Wrap(new byte[] { 0xA }), new SessionWindow(long.MaxValue, long.MaxValue)))
//            );
//        }

//        [Fact]
//        public void TestUpperBoundWithKeyBytesLargerThanFirstTimestampByte()
//        {
//            Bytes upper = sessionKeySchema.upperRange(Bytes.Wrap(new byte[] { 0xA, (byte)0x8F, (byte)0x9F }), long.MaxValue);

//            Assert.Equal(
//                "shorter key with max timestamp should be in range",
//                upper.CompareTo(SessionKeySchema.toBinary(
//                    new Windowed<>(
//                        Bytes.Wrap(new byte[] { 0xA, (byte)0x8F }),
//                        new SessionWindow(long.MaxValue, long.MaxValue))
//                    )
//                ) >= 0
//            );

//            Assert.Equal(upper, SessionKeySchema.toBinary(
//                new Windowed<>(Bytes.Wrap(new byte[] { 0xA, (byte)0x8F, (byte)0x9F }), new SessionWindow(long.MaxValue, long.MaxValue)))
//            );
//        }

//        [Fact]
//        public void TestUpperBoundWithZeroTimestamp()
//        {
//            Bytes upper = sessionKeySchema.upperRange(Bytes.Wrap(new byte[] { 0xA, 0xB, 0xC }), 0);

//            Assert.Equal(upper, SessionKeySchema.toBinary(
//                new Windowed<>(Bytes.Wrap(new byte[] { 0xA }), new SessionWindow(0, long.MaxValue)))
//            );
//        }

//        [Fact]
//        public void TestLowerBoundWithZeroTimestamp()
//        {
//            Bytes lower = sessionKeySchema.lowerRange(Bytes.Wrap(new byte[] { 0xA, 0xB, 0xC }), 0);
//            Assert.Equal(lower, SessionKeySchema.toBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0xA, 0xB, 0xC }), new SessionWindow(0, 0))));
//        }

//        [Fact]
//        public void TestLowerBoundMatchesTrailingZeros()
//        {
//            Bytes lower = sessionKeySchema.lowerRange(Bytes.Wrap(new byte[] { 0xA, 0xB, 0xC }), long.MaxValue);

//            Assert.Equal(
//                "appending zeros to key should still be in range",
//                lower.CompareTo(SessionKeySchema.toBinary(
//                    new Windowed<>(
//                        Bytes.Wrap(new byte[] { 0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }),
//                        new SessionWindow(long.MaxValue, long.MaxValue))
//                )) < 0
//            );

//            Assert.Equal(lower, SessionKeySchema.toBinary(new Windowed<>(Bytes.Wrap(new byte[] { 0xA, 0xB, 0xC }), new SessionWindow(0, 0))));
//        }

//        [Fact]
//        public void ShouldSerializeDeserialize()
//        {
//            byte[] bytes = keySerde.Serializer.Serialize(topic, windowedKey);
//            IWindowed<string> result = keySerde.Deserializer.Deserialize(topic, bytes);
//            Assert.Equal(windowedKey, result);
//        }

//        [Fact]
//        public void ShouldSerializeNullToNull()
//        {
//            Assert.Null(keySerde.Serializer.Serialize(topic, null));
//        }

//        [Fact]
//        public void ShouldDeSerializeEmtpyByteArrayToNull()
//        {
//            Assert.Null(keySerde.Deserializer.Deserialize(topic, System.Array.Empty<byte>()));
//        }

//        [Fact]
//        public void ShouldDeSerializeNullToNull()
//        {
//            Assert.Null(keySerde.Deserializer.Deserialize(topic, null));
//        }

//        [Fact]
//        public void ShouldConvertToBinaryAndBack()
//        {
//            byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.Serializer, "dummy");
//            IWindowed<string> result = SessionKeySchema.from(serialized, Serdes.String().Deserializer, "dummy");
//            Assert.Equal(windowedKey, result);
//        }

//        [Fact]
//        public void ShouldExtractEndTimeFromBinary()
//        {
//            byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.Serializer, "dummy");
//            Assert.Equal(endTime, SessionKeySchema.extractEndTimestamp(serialized));
//        }

//        [Fact]
//        public void ShouldExtractStartTimeFromBinary()
//        {
//            byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.Serializer, "dummy");
//            Assert.Equal(startTime, SessionKeySchema.extractStartTimestamp(serialized));
//        }

//        [Fact]
//        public void ShouldExtractWindowFromBindary()
//        {
//            byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.Serializer, "dummy");
//            Assert.Equal(window, SessionKeySchema.extractWindow(serialized));
//        }

//        [Fact]
//        public void ShouldExtractKeyBytesFromBinary()
//        {
//            byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.Serializer, "dummy");
//            assertArrayEquals(key.getBytes(), SessionKeySchema.extractKeyBytes(serialized));
//        }

//        [Fact]
//        public void ShouldExtractKeyFromBinary()
//        {
//            byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.Serializer, "dummy");
//            Assert.Equal(windowedKey, SessionKeySchema.from(serialized, serde.Deserializer, "dummy"));
//        }

//        [Fact]
//        public void ShouldExtractBytesKeyFromBinary()
//        {
//            Bytes bytesKey = Bytes.Wrap(key.getBytes());
//            IWindowed<Bytes> windowedBytesKey = new Windowed<>(bytesKey, window);
//            Bytes serialized = SessionKeySchema.toBinary(windowedBytesKey);
//            Assert.Equal(windowedBytesKey, SessionKeySchema.from(serialized));
//        }

//        private List<int> GetValues(HasNextCondition hasNextCondition)
//        {
//            List<int> results = new List<int>();
//            while (hasNextCondition.hasNext(iterator))
//            {
//                results.Add(iterator.MoveNext().Value);
//            }
//            return results;
//        }

//    }
//}
///*






//*

//*





//*/

























