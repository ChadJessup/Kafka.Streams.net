namespace Kafka.Streams.Tests.State.Internals
{
    /*






    *

    *





    */












    // TODO: this test coverage does not consider session serde yet
    public class SegmentedCacheFunctionTest
    {

        private static readonly int SEGMENT_INTERVAL = 17;
        private static readonly int TIMESTAMP = 736213517;

        private static Bytes THE_KEY = WindowKeySchema.toStoreKeyBinary(new byte[] { 0xA, 0xB, 0xC }, TIMESTAMP, 42);
        private static Bytes THE_CACHE_KEY = Bytes.wrap(
            ByteBuffer.allocate(8 + THE_KEY.get().Length)
                .putLong(TIMESTAMP / SEGMENT_INTERVAL)
                .put(THE_KEY.get()).array()
        );

        private SegmentedCacheFunction cacheFunction = new SegmentedCacheFunction(new WindowKeySchema(), SEGMENT_INTERVAL);

        [Xunit.Fact]
        public void Key()
        {
            Assert.Equal(
                cacheFunction.key(THE_CACHE_KEY),
                equalTo(THE_KEY)
            );
        }

        [Xunit.Fact]
        public void CacheKey()
        {
            long segmentId = TIMESTAMP / SEGMENT_INTERVAL;

            Bytes actualCacheKey = cacheFunction.cacheKey(THE_KEY);
            ByteBuffer buffer = ByteBuffer.wrap(actualCacheKey.get());

            Assert.Equal(buffer.getLong(), (segmentId));

            byte[] actualKey = new byte[buffer.remaining()];
            buffer.get(actualKey);
            Assert.Equal(Bytes.wrap(actualKey), (THE_KEY));
        }

        [Xunit.Fact]
        public void TestRoundTripping()
        {
            Assert.Equal(
                cacheFunction.key(cacheFunction.cacheKey(THE_KEY)),
                equalTo(THE_KEY)
            );

            Assert.Equal(
                cacheFunction.cacheKey(cacheFunction.key(THE_CACHE_KEY)),
                equalTo(THE_CACHE_KEY)
            );
        }

        [Xunit.Fact]
        public void CompareSegmentedKeys()
        {
            Assert.Equal(
                "same key in same segment should be ranked the same",
                cacheFunction.compareSegmentedKeys(
                    cacheFunction.cacheKey(THE_KEY),
                    THE_KEY
                ) == 0
            );

            Bytes sameKeyInPriorSegment = WindowKeySchema.toStoreKeyBinary(new byte[] { 0xA, 0xB, 0xC }, 1234, 42);

            Assert.Equal(
                "same keys in different segments should be ordered according to segment",
                cacheFunction.compareSegmentedKeys(
                    cacheFunction.cacheKey(sameKeyInPriorSegment),
                    THE_KEY
                ) < 0
            );

            Assert.Equal(
                "same keys in different segments should be ordered according to segment",
                cacheFunction.compareSegmentedKeys(
                    cacheFunction.cacheKey(THE_KEY),
                    sameKeyInPriorSegment
                ) > 0
            );

            Bytes lowerKeyInSameSegment = WindowKeySchema.toStoreKeyBinary(new byte[] { 0xA, 0xB, 0xB }, TIMESTAMP - 1, 0);

            Assert.Equal(
                "different keys in same segments should be ordered according to key",
                cacheFunction.compareSegmentedKeys(
                    cacheFunction.cacheKey(THE_KEY),
                    lowerKeyInSameSegment
                ) > 0
            );

            Assert.Equal(
                "different keys in same segments should be ordered according to key",
                cacheFunction.compareSegmentedKeys(
                    cacheFunction.cacheKey(lowerKeyInSameSegment),
                    THE_KEY
                ) < 0
            );
        }

    }
}
/*






*

*





*/












// TODO: this test coverage does not consider session serde yet
