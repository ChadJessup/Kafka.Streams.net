//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State.KeyValues;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class CompositeReadOnlyKeyValueStoreTest
//    {
//        private readonly string storeName = "my-store";
//        private readonly string storeNameA = "my-storeA";
//        private StateStoreProviderStub stubProviderTwo;
//        private IKeyValueStore<string, string> stubOneUnderlying;
//        private IKeyValueStore<string, string> otherUnderlyingStore;
//        private CompositeReadOnlyKeyValueStore<string, string> theStore;


//        public void Before()
//        {
//            StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
//            stubProviderTwo = new StateStoreProviderStub(false);

//            stubOneUnderlying = NewStoreInstance();
//            stubProviderOne.addStore(storeName, stubOneUnderlying);
//            otherUnderlyingStore = NewStoreInstance();
//            stubProviderOne.addStore("other-store", otherUnderlyingStore);

//            theStore = new CompositeReadOnlyKeyValueStore<>(
//                new WrappingStoreProvider(Array.< StateStoreProvider > asList(stubProviderOne, stubProviderTwo)),
//                                            QueryableStoreTypes.< string, string > KeyValueStore(),
//                                            storeName);
//        }

//        private IKeyValueStore<string, string> NewStoreInstance()
//        {
//            IKeyValueStore<string, string> store = Stores.KeyValueStoreBuilder(Stores.InMemoryKeyValueStore(storeName),
//                    Serdes.String(),
//                    Serdes.String())
//                    .Build();

//            store.Init(new InternalMockProcessorContext(new StateSerdes<>(ProcessorStateManager.storeChangelogTopic("appId", storeName), Serdes.String(), Serdes.String()),
//                                                        new NoOpRecordCollector()),
//                    store);

//            return store;
//        }

//        [Fact]
//        public void ShouldReturnNullIfKeyDoesntExist()
//        {
//            Assert.Null(theStore.Get("whatever"));
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnGetNullKey()
//        {
//            theStore.Get(null);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnRangeNullFromKey()
//        {
//            theStore.Range(null, "to");
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionOnRangeNullToKey()
//        {
//            theStore.Range("from", null);
//        }

//        [Fact]
//        public void ShouldReturnValueIfExists()
//        {
//            stubOneUnderlying.Put("key", "value");
//            Assert.Equal("value", theStore.Get("key"));
//        }

//        [Fact]
//        public void ShouldNotGetValuesFromOtherStores()
//        {
//            otherUnderlyingStore.Put("otherKey", "otherValue");
//            Assert.Null(theStore.Get("otherKey"));
//        }

//        [Fact]
//        public void ShouldThrowNoSuchElementExceptionWhileNext()
//        {
//            stubOneUnderlying.Put("a", "1");
//            IKeyValueIterator<string, string> keyValueIterator = theStore.Range("a", "b");
//            keyValueIterator.MoveNext();
//            try
//            {
//                keyValueIterator.MoveNext();
//                Assert.True(false, "Should have thrown NoSuchElementException with next()");
//            }
//            catch (NoSuchElementException e) { }
//        }

//        [Fact]
//        public void ShouldThrowNoSuchElementExceptionWhilePeekNext()
//        {
//            stubOneUnderlying.Put("a", "1");
//            IKeyValueIterator<string, string> keyValueIterator = theStore.Range("a", "b");
//            keyValueIterator.MoveNext();
//            try
//            {
//                keyValueIterator.PeekNextKey();
//                Assert.True(false, "Should have thrown NoSuchElementException with PeekNextKey()");
//            }
//            catch (NoSuchElementException e) { }
//        }

//        [Fact]
//        public void ShouldThrowUnsupportedOperationExceptionWhileRemove()
//        {
//            IKeyValueIterator<string, string> keyValueIterator = theStore.All();
//            try
//            {
//                keyValueIterator.remove();
//                Assert.True(false, "Should have thrown UnsupportedOperationException");
//            }
//            catch (UnsupportedOperationException e) { }
//        }

//        [Fact]
//        public void ShouldThrowUnsupportedOperationExceptionWhileRange()
//        {
//            stubOneUnderlying.Put("a", "1");
//            stubOneUnderlying.Put("b", "1");
//            IKeyValueIterator<string, string> keyValueIterator = theStore.Range("a", "b");
//            try
//            {
//                keyValueIterator.remove();
//                Assert.True(false, "Should have thrown UnsupportedOperationException");
//            }
//            catch (UnsupportedOperationException e) { }
//        }

//        [Fact]
//        public void ShouldFindValueForKeyWhenMultiStores()
//        {
//            IKeyValueStore<string, string> cache = NewStoreInstance();
//            stubProviderTwo.addStore(storeName, cache);

//            cache.Put("key-two", "key-two-value");
//            stubOneUnderlying.Put("key-one", "key-one-value");

//            Assert.Equal("key-two-value", theStore.Get("key-two"));
//            Assert.Equal("key-one-value", theStore.Get("key-one"));
//        }

//        [Fact]
//        public void ShouldSupportRange()
//        {
//            stubOneUnderlying.Put("a", "a");
//            stubOneUnderlying.Put("b", "b");
//            stubOneUnderlying.Put("c", "c");

//            List<KeyValuePair<string, string>> results = toList(theStore.Range("a", "b"));
//            Assert.Contains(new KeyValuePair<string, string>("a", "a"), results);
//            Assert.Contains(new KeyValuePair<string, string>("b", "b"), results);
//            Assert.Equal(2, results.Count);
//        }

//        [Fact]
//        public void ShouldSupportRangeAcrossMultipleKVStores()
//        {
//            IKeyValueStore<string, string> cache = NewStoreInstance();
//            stubProviderTwo.addStore(storeName, cache);

//            stubOneUnderlying.Put("a", "a");
//            stubOneUnderlying.Put("b", "b");
//            stubOneUnderlying.Put("z", "z");

//            cache.Put("c", "c");
//            cache.Put("d", "d");
//            cache.Put("x", "x");

//            List<KeyValuePair<string, string>> results = toList(theStore.Range("a", "e"));
//            Assert.Contains(new KeyValuePair<string, string>("a", "a"), results);
//            Assert.Contains(new KeyValuePair<string, string>("b", "b"), results);
//            Assert.Contains(new KeyValuePair<string, string>("c", "c"), results);
//            Assert.Contains(new KeyValuePair<string, string>("d", "d"), results);
//            Assert.Equal(4, results.Count);
//        }

//        [Fact]
//        public void ShouldSupportAllAcrossMultipleStores()
//        {
//            IKeyValueStore<string, string> cache = NewStoreInstance();
//            stubProviderTwo.addStore(storeName, cache);

//            stubOneUnderlying.Put("a", "a");
//            stubOneUnderlying.Put("b", "b");
//            stubOneUnderlying.Put("z", "z");

//            cache.Put("c", "c");
//            cache.Put("d", "d");
//            cache.Put("x", "x");

//            List<KeyValuePair<string, string>> results = toList(theStore.All());
//            Assert.Contains(new KeyValuePair<string, string>("a", "a"), results);
//            Assert.Contains(new KeyValuePair<string, string>("b", "b"), results);
//            Assert.Contains(new KeyValuePair<string, string>("c", "c"), results);
//            Assert.Contains(new KeyValuePair<string, string>("d", "d"), results);
//            Assert.Contains(new KeyValuePair<string, string>("x", "x"), results);
//            Assert.Contains(new KeyValuePair<string, string>("z", "z"), results);
//            Assert.Equal(6, results.Count);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowInvalidStoreExceptionDuringRebalance()
//        {
//            rebalancing().Get("anything");
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowInvalidStoreExceptionOnApproximateNumEntriesDuringRebalance()
//        {
//            rebalancing().approximateNumEntries;
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowInvalidStoreExceptionOnRangeDuringRebalance()
//        {
//            rebalancing().Range("anything", "something");
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowInvalidStoreExceptionOnAllDuringRebalance()
//        {
//            rebalancing().All();
//        }

//        [Fact]
//        public void ShouldGetApproximateEntriesAcrossAllStores()
//        {
//            IKeyValueStore<string, string> cache = NewStoreInstance();
//            stubProviderTwo.addStore(storeName, cache);

//            stubOneUnderlying.Put("a", "a");
//            stubOneUnderlying.Put("b", "b");
//            stubOneUnderlying.Put("z", "z");

//            cache.Put("c", "c");
//            cache.Put("d", "d");
//            cache.Put("x", "x");

//            Assert.Equal(6, theStore.approximateNumEntries);
//        }

//        [Fact]
//        public void ShouldReturnLongMaxValueOnOverflow()
//        {
//            stubProviderTwo.addStore(storeName, new NoOpReadOnlyStore<object, object>()
//            {


//            public long approximateNumEntries
//            {
//                return long.MaxValue;
//            }
//        });

//        stubOneUnderlying.Put("overflow", "me");
//        Assert.Equal(long.MaxValue, theStore.approximateNumEntries);
//    }

//    [Fact]
//    public void ShouldReturnLongMaxValueOnUnderflow()
//    {
//        stubProviderTwo.addStore(storeName, new NoOpReadOnlyStore<object, object>()
//        {


//            public long approximateNumEntries
//        {
//            return long.MaxValue;
//        }
//    });
//        stubProviderTwo.addStore(storeNameA, new NoOpReadOnlyStore<object, object>() {
            
//            public long ApproximateNumEntries()
//    {
//        return long.MaxValue;
//    }
//});

//        Assert.Equal(long.MaxValue, theStore.approximateNumEntries);
//    }

//    private CompositeReadOnlyKeyValueStore<object, object> Rebalancing()
//{
//    return new CompositeReadOnlyKeyValueStore<>(new WrappingStoreProvider(Collections.< StateStoreProvider > singletonList(new StateStoreProviderStub(true))),
//            QueryableStoreTypes.KeyValueStore(), storeName);
//}

//}}
