//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

























//    public class CompositeReadOnlySessionStoreTest
//    {

//        private readonly string storeName = "session-store";
//        private StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
//        private StateStoreProviderStub stubProviderTwo = new StateStoreProviderStub(false);
//        private ReadOnlySessionStoreStub<string, long> underlyingSessionStore = new ReadOnlySessionStoreStub<>();
//        private ReadOnlySessionStoreStub<string, long> otherUnderlyingStore = new ReadOnlySessionStoreStub<>();
//        private CompositeReadOnlySessionStore<string, long> sessionStore;


//        public void Before()
//        {
//            stubProviderOne.addStore(storeName, underlyingSessionStore);
//            stubProviderOne.addStore("other-session-store", otherUnderlyingStore);


//            sessionStore = new CompositeReadOnlySessionStore<>(
//                    new WrappingStoreProvider(Array.< StateStoreProvider > asList(stubProviderOne, stubProviderTwo)),
//                    QueryableStoreTypes.< string, long > sessionStore(), storeName);
//        }

//        [Fact]
//        public void ShouldFetchResulstFromUnderlyingSessionStore()
//        {
//            underlyingSessionStore.Put(new IWindowed<>("a", new SessionWindow(0, 0)), 1L);
//            underlyingSessionStore.Put(new IWindowed<>("a", new SessionWindow(10, 10)), 2L);

//            List<KeyValuePair<IWindowed<string>, long>> results = toList(sessionStore.Fetch("a"));
//            Assert.Equal(Array.asList(KeyValuePair.Create(new IWindowed<>("a", new SessionWindow(0, 0)), 1L),
//                                       KeyValuePair.Create(new IWindowed<>("a", new SessionWindow(10, 10)), 2L)),
//                         results);
//        }

//        [Fact]
//        public void ShouldReturnEmptyIteratorIfNoData()
//        {
//            IKeyValueIterator<IWindowed<string>, long> result = sessionStore.Fetch("b");
//            Assert.False(result.HasNext());
//        }

//        [Fact]
//        public void ShouldFindValueForKeyWhenMultiStores()
//        {
//            ReadOnlySessionStoreStub<string, long> secondUnderlying = new
//                    ReadOnlySessionStoreStub<>();
//            stubProviderTwo.addStore(storeName, secondUnderlying);

//            IWindowed<string> keyOne = new IWindowed<>("key-one", new SessionWindow(0, 0));
//            IWindowed<string> keyTwo = new IWindowed<>("key-two", new SessionWindow(0, 0));
//            underlyingSessionStore.Put(keyOne, 0L);
//            secondUnderlying.Put(keyTwo, 10L);

//            List<KeyValuePair<IWindowed<string>, long>> keyOneResults = toList(sessionStore.Fetch("key-one"));
//            List<KeyValuePair<IWindowed<string>, long>> keyTwoResults = toList(sessionStore.Fetch("key-two"));

//            Assert.Equal(Collections.singletonList(KeyValuePair.Create(keyOne, 0L)), keyOneResults);
//            Assert.Equal(Collections.singletonList(KeyValuePair.Create(keyTwo, 10L)), keyTwoResults);
//        }

//        [Fact]
//        public void ShouldNotGetValueFromOtherStores()
//        {
//            IWindowed<string> expectedKey = new IWindowed<>("foo", new SessionWindow(0, 0));
//            otherUnderlyingStore.Put(new IWindowed<>("foo", new SessionWindow(10, 10)), 10L);
//            underlyingSessionStore.Put(expectedKey, 1L);

//            IKeyValueIterator<IWindowed<string>, long> result = sessionStore.Fetch("foo");
//            Assert.Equal(KeyValuePair.Create(expectedKey, 1L), result.MoveNext());
//            Assert.False(result.HasNext());
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowInvalidStateStoreExceptionOnRebalance()
//        {
//            CompositeReadOnlySessionStore<string, string> store =
//                new CompositeReadOnlySessionStore<>(
//                    new StateStoreProviderStub(true),
//                    QueryableStoreTypes.sessionStore(),
//                    "whateva");

//            store.Fetch("a");
//        }

//        [Fact]
//        public void ShouldThrowInvalidStateStoreExceptionIfSessionFetchThrows()
//        {
//            underlyingSessionStore.setOpen(false);
//            try
//            {
//                sessionStore.Fetch("key");
//                Assert.True(false, "Should have thrown InvalidStateStoreException with session store");
//            }
//            catch (InvalidStateStoreException e) { }
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerExceptionIfFetchingNullKey()
//        {
//            sessionStore.Fetch(null);
//        }

//        [Fact]
//        public void ShouldFetchKeyRangeAcrossStores()
//        {
//            ReadOnlySessionStoreStub<string, long> secondUnderlying = new
//                    ReadOnlySessionStoreStub<>();
//            stubProviderTwo.addStore(storeName, secondUnderlying);
//            underlyingSessionStore.Put(new IWindowed<>("a", new SessionWindow(0, 0)), 0L);
//            secondUnderlying.Put(new IWindowed<>("b", new SessionWindow(0, 0)), 10L);
//            List<KeyValuePair<IWindowed<string>, long>> results = StreamsTestUtils.toList(sessionStore.Fetch("a", "b"));
//            Assert.Equal(results.Count, (2));
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNPEIfKeyIsNull()
//        {
//            underlyingSessionStore.Fetch(null);
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNPEIfFromKeyIsNull()
//        {
//            underlyingSessionStore.Fetch(null, "a");
//        }

//        [Fact]// (expected = NullPointerException)
//        public void ShouldThrowNPEIfToKeyIsNull()
//        {
//            underlyingSessionStore.Fetch("a", null);
//        }
//    }
//}
///*






//*

//*





//*/

























