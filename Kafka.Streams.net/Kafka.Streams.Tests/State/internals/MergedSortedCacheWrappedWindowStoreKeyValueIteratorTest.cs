//using Confluent.Kafka;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State.Internals;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class MergedSortedCacheWrappedWindowStoreKeyValueIteratorTest
//    {
//        //    private static SegmentedCacheFunction SINGLE_SEGMENT_CACHE_FUNCTION = new SegmentedCacheFunction(null, -1)
//        //    {


//        //    public long SegmentId(Bytes key)
//        //    {
//        //        return 0;
//        //    }
//        //};
//        private static int WINDOW_SIZE = 10;

//        private string storeKey = "a";
//        private string cacheKey = "b";

//        private TimeWindow storeWindow = new TimeWindow(0, 1);
//        private Iterator<KeyValuePair<IWindowed<Bytes>, byte[]>> storeKvs = Collections.singleton(
//            KeyValuePair.Create(new Windowed2<>(Bytes.Wrap(storeKey.getBytes()), storeWindow), storeKey.getBytes())).iterator();

//        private TimeWindow cacheWindow = new TimeWindow(10, 20);
//        private Iterator<KeyValuePair<Bytes, LRUCacheEntry>> cacheKvs = Collections.singleton(
//            KeyValuePair.Create(
//                SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(WindowKeySchema.toStoreKeyBinary(
//                        new Windowed2<K>(cacheKey, cacheWindow), 0, new StateSerdes<>("dummy", Serdes.String(), Serdes.ByteArray()))
//                ),
//                new LRUCacheEntry(cacheKey.getBytes())
//            )).iterator();

//        private IDeserializer<string> deserializer = Serdes.String().Deserializer;

//        [Fact]
//        public void ShouldHaveNextFromStore()
//        {
//            MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
//                createIterator(storeKvs, Collections.emptyIterator());
//            Assert.True(mergeIterator.HasNext());
//        }

//        [Fact]
//        public void ShouldGetNextFromStore()
//        {
//            MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
//                createIterator(storeKvs, Collections.emptyIterator());
//            Assert.Equal(convertKeyValuePair(mergeIterator.MoveNext()), (KeyValuePair.Create(new Windowed2<>(storeKey, storeWindow), storeKey)));
//        }

//        [Fact]
//        public void ShouldPeekNextKeyFromStore()
//        {
//            MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
//                createIterator(storeKvs, Collections.emptyIterator());
//            Assert.Equal(convertWindowedKey(mergeIterator.PeekNextKey()), (new Windowed2<>(storeKey, storeWindow)));
//        }

//        [Fact]
//        public void ShouldHaveNextFromCache()
//        {
//            MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
//                createIterator(Collections.emptyIterator(), cacheKvs);
//            Assert.True(mergeIterator.HasNext());
//        }

//        [Fact]
//        public void ShouldGetNextFromCache()
//        {
//            MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
//                createIterator(Collections.emptyIterator(), cacheKvs);
//            Assert.Equal(convertKeyValuePair(mergeIterator.MoveNext()), (KeyValuePair.Create(new Windowed2<>(cacheKey, cacheWindow), cacheKey)));
//        }

//        [Fact]
//        public void ShouldPeekNextKeyFromCache()
//        {
//            MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
//                createIterator(Collections.emptyIterator(), cacheKvs);
//            Assert.Equal(convertWindowedKey(mergeIterator.PeekNextKey()), (new Windowed2<string>(cacheKey, cacheWindow)));
//        }

//        [Fact]
//        public void ShouldIterateBothStoreAndCache()
//        {
//            MergedSortedCacheWindowStoreKeyValueIterator iterator = createIterator(storeKvs, cacheKvs);
//            Assert.Equal(convertKeyValuePair(iterator.MoveNext()), (KeyValuePair.Create(new Windowed2<string>(storeKey, storeWindow), storeKey)));
//            Assert.Equal(convertKeyValuePair(iterator.MoveNext()), (KeyValuePair.Create(new Windowed2<>(cacheKey, cacheWindow), cacheKey)));
//            Assert.False(iterator.HasNext());
//        }

//        private KeyValuePair<IWindowed<string>, string> ConvertKeyValuePair(KeyValuePair<IWindowed<Bytes>, byte[]> next)
//        {
//            string value = deserializer.Deserialize("", next.Value);
//            return KeyValuePair.Create(convertWindowedKey(next.Key), value);
//        }

//        private IWindowed<string> ConvertWindowedKey(IWindowed<Bytes> bytesWindowed)
//        {
//            string key = deserializer.Deserialize("", bytesWindowed.Key.Get());
//            return new Windowed2<string>(key, bytesWindowed.window());
//        }

//        private MergedSortedCacheWindowStoreKeyValueIterator CreateIterator(
//            Iterator<KeyValuePair<IWindowed<Bytes>, byte[]>> storeKvs,
//            Iterator<KeyValuePair<Bytes, LRUCacheEntry>> cacheKvs
//        )
//        {
//            DelegatingPeekingKeyValueIterator<IWindowed<Bytes>, byte[]> storeIterator =
//                new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(storeKvs));

//            PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator =
//                new DelegatingPeekingKeyValueIterator<>("cache", new KeyValueIteratorStub<>(cacheKvs));
//            return new MergedSortedCacheWindowStoreKeyValueIterator(
//                cacheIterator,
//                storeIterator,
//                new StateSerdes<>("Name", Serdes.ByteArray(), Serdes.ByteArray()),
//                WINDOW_SIZE,
//                SINGLE_SEGMENT_CACHE_FUNCTION
//            );
//        }
//    }
//}
