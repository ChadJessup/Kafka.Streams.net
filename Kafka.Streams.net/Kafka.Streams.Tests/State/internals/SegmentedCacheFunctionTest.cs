//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */












//    // TODO: this test coverage does not consider session serde yet
//    public class SegmentedCacheFunctionTest
//    {

//        private const int SEGMENT_INTERVAL = 17;
//        private const int TIMESTAMP = 736213517;

//        private static Bytes THE_KEY = WindowKeySchema.toStoreKeyBinary(new byte[] { 0xA, 0xB, 0xC }, TIMESTAMP, 42);
//        private static Bytes THE_CACHE_KEY = Bytes.Wrap(
//            new ByteBuffer().Allocate(8 + THE_KEY.Get().Length)
//                .putLong(TIMESTAMP / SEGMENT_INTERVAL)
//                .Put(THE_KEY.Get()).array()
//        );

//        private SegmentedCacheFunction cacheFunction = new SegmentedCacheFunction(new WindowKeySchema(), SEGMENT_INTERVAL);

//        [Fact]
//        public void Key()
//        {
//            Assert.Equal(
//                cacheFunction.Key(THE_CACHE_KEY),
//                equalTo(THE_KEY)
//            );
//        }

//        [Fact]
//        public void CacheKey()
//        {
//            long segmentId = TIMESTAMP / SEGMENT_INTERVAL;

//            Bytes actualCacheKey = cacheFunction.cacheKey(THE_KEY);
//            ByteBuffer buffer = new ByteBuffer().Wrap(actualCacheKey.Get());

//            Assert.Equal(buffer.GetLong(), segmentId);

//            byte[] actualKey = new byte[buffer.remaining()];
//            buffer.Get(actualKey);
//            Assert.Equal(Bytes.Wrap(actualKey), THE_KEY);
//        }

//        [Fact]
//        public void TestRoundTripping()
//        {
//            Assert.Equal(
//                cacheFunction.Key(cacheFunction.cacheKey(THE_KEY)),
//                equalTo(THE_KEY)
//            );

//            Assert.Equal(
//                cacheFunction.cacheKey(cacheFunction.Key(THE_CACHE_KEY)),
//                equalTo(THE_CACHE_KEY)
//            );
//        }

//        [Fact]
//        public void CompareSegmentedKeys()
//        {
//            Assert.Equal(
//                "same key in same segment should be ranked the same",
//                cacheFunction.compareSegmentedKeys(
//                    cacheFunction.cacheKey(THE_KEY),
//                    THE_KEY
//                ) == 0
//            );

//            Bytes sameKeyInPriorSegment = WindowKeySchema.toStoreKeyBinary(new byte[] { 0xA, 0xB, 0xC }, 1234, 42);

//            Assert.Equal(
//                "same keys in different segments should be ordered according to segment",
//                cacheFunction.compareSegmentedKeys(
//                    cacheFunction.cacheKey(sameKeyInPriorSegment),
//                    THE_KEY
//                ) < 0
//            );

//            Assert.Equal(
//                "same keys in different segments should be ordered according to segment",
//                cacheFunction.compareSegmentedKeys(
//                    cacheFunction.cacheKey(THE_KEY),
//                    sameKeyInPriorSegment
//                ) > 0
//            );

//            Bytes lowerKeyInSameSegment = WindowKeySchema.toStoreKeyBinary(new byte[] { 0xA, 0xB, 0xB }, TIMESTAMP - 1, 0);

//            Assert.Equal(
//                "different keys in same segments should be ordered according to key",
//                cacheFunction.compareSegmentedKeys(
//                    cacheFunction.cacheKey(THE_KEY),
//                    lowerKeyInSameSegment
//                ) > 0
//            );

//            Assert.Equal(
//                "different keys in same segments should be ordered according to key",
//                cacheFunction.compareSegmentedKeys(
//                    cacheFunction.cacheKey(lowerKeyInSameSegment),
//                    THE_KEY
//                ) < 0
//            );
//        }

//    }
//}
///*






//*

//*





//*/












//// TODO: this test coverage does not consider session serde yet
