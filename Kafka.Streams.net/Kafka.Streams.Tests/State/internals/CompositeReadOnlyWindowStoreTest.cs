namespace Kafka.Streams.Tests.State.Internals
{
    /*






    *

    *





    */


























    public class CompositeReadOnlyWindowStoreTest
    {

        private static readonly long WINDOW_SIZE = 30_000;

        private readonly string storeName = "window-store";
        private StateStoreProviderStub stubProviderOne;
        private StateStoreProviderStub stubProviderTwo;
        private CompositeReadOnlyWindowStore<string, string> windowStore;
        private ReadOnlyWindowStoreStub<string, string> underlyingWindowStore;
        private ReadOnlyWindowStoreStub<string, string> otherUnderlyingStore;


        public void Before()
        {
            stubProviderOne = new StateStoreProviderStub(false);
            stubProviderTwo = new StateStoreProviderStub(false);
            underlyingWindowStore = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
            stubProviderOne.addStore(storeName, underlyingWindowStore);

            otherUnderlyingStore = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
            stubProviderOne.addStore("other-window-store", otherUnderlyingStore);


            windowStore = new CompositeReadOnlyWindowStore<>(
                new WrappingStoreProvider(Array.< StateStoreProvider > asList(stubProviderOne, stubProviderTwo)),
                    QueryableStoreTypes.< string, string > windowStore(),
                    storeName);
        }

        [Xunit.Fact]
        public void ShouldFetchValuesFromWindowStore()
        {
            underlyingWindowStore.put("my-key", "my-value", 0L);
            underlyingWindowStore.put("my-key", "my-later-value", 10L);

            WindowStoreIterator<string> iterator = windowStore.fetch("my-key", ofEpochMilli(0L), ofEpochMilli(25L));
            List<KeyValuePair<long, string>> results = StreamsTestUtils.toList(iterator);

            Assert.Equal(asList(new KeyValuePair<>(0L, "my-value"),
                                new KeyValuePair<>(10L, "my-later-value")),
                         results);
        }

        [Xunit.Fact]
        public void ShouldReturnEmptyIteratorIfNoData()
        {
            WindowStoreIterator<string> iterator = windowStore.fetch("my-key", ofEpochMilli(0L), ofEpochMilli(25L));
            Assert.Equal(false, iterator.hasNext());
        }

        [Xunit.Fact]
        public void ShouldFindValueForKeyWhenMultiStores()
        {
            ReadOnlyWindowStoreStub<string, string> secondUnderlying = new
                ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
            stubProviderTwo.addStore(storeName, secondUnderlying);

            underlyingWindowStore.put("key-one", "value-one", 0L);
            secondUnderlying.put("key-two", "value-two", 10L);

            List<KeyValuePair<long, string>> keyOneResults = StreamsTestUtils.toList(windowStore.fetch("key-one", ofEpochMilli(0L),
                                                                                                         ofEpochMilli(1L)));
            List<KeyValuePair<long, string>> keyTwoResults = StreamsTestUtils.toList(windowStore.fetch("key-two", ofEpochMilli(10L),
                                                                                                         ofEpochMilli(11L)));

            Assert.Equal(Collections.singletonList(KeyValuePair.Create(0L, "value-one")), keyOneResults);
            Assert.Equal(Collections.singletonList(KeyValuePair.Create(10L, "value-two")), keyTwoResults);
        }

        [Xunit.Fact]
        public void ShouldNotGetValuesFromOtherStores()
        {
            otherUnderlyingStore.put("some-key", "some-value", 0L);
            underlyingWindowStore.put("some-key", "my-value", 1L);

            List<KeyValuePair<long, string>> results = StreamsTestUtils.toList(windowStore.fetch("some-key", ofEpochMilli(0L), ofEpochMilli(2L)));
            Assert.Equal(Collections.singletonList(new KeyValuePair<>(1L, "my-value")), results);
        }

        [Xunit.Fact]// (expected = InvalidStateStoreException)
        public void ShouldThrowInvalidStateStoreExceptionOnRebalance()
        {
            CompositeReadOnlyWindowStore<object, object> store = new CompositeReadOnlyWindowStore<>(new StateStoreProviderStub(true), QueryableStoreTypes.windowStore(), "foo");
            store.fetch("key", ofEpochMilli(1), ofEpochMilli(10));
        }

        [Xunit.Fact]
        public void ShouldThrowInvalidStateStoreExceptionIfFetchThrows()
        {
            underlyingWindowStore.setOpen(false);
            CompositeReadOnlyWindowStore<object, object> store =
                    new CompositeReadOnlyWindowStore<>(stubProviderOne, QueryableStoreTypes.windowStore(), "window-store");
            try
            {
                store.fetch("key", ofEpochMilli(1), ofEpochMilli(10));
                Assert.True(false, "InvalidStateStoreException was expected");
            }
            catch (InvalidStateStoreException e)
            {
                Assert.Equal("State store is not available anymore and may have been migrated to another instance; " +
                        "please re-discover its location from the state metadata.", e.getMessage());
            }
        }

        [Xunit.Fact]
        public void EmptyIteratorAlwaysReturnsFalse()
        {
            CompositeReadOnlyWindowStore<object, object> store = new CompositeReadOnlyWindowStore<>(new
                    StateStoreProviderStub(false), QueryableStoreTypes.windowStore(), "foo");
            WindowStoreIterator<object> windowStoreIterator = store.fetch("key", ofEpochMilli(1), ofEpochMilli(10));

            Assert.False(windowStoreIterator.hasNext());
        }

        [Xunit.Fact]
        public void EmptyIteratorPeekNextKeyShouldThrowNoSuchElementException()
        {
            CompositeReadOnlyWindowStore<object, object> store = new CompositeReadOnlyWindowStore<>(new
                    StateStoreProviderStub(false), QueryableStoreTypes.windowStore(), "foo");
            WindowStoreIterator<object> windowStoreIterator = store.fetch("key", ofEpochMilli(1), ofEpochMilli(10));
            assertThrows(NoSuchElementException, windowStoreIterator::peekNextKey);
        }

        [Xunit.Fact]
        public void EmptyIteratorNextShouldThrowNoSuchElementException()
        {
            CompositeReadOnlyWindowStore<object, object> store = new CompositeReadOnlyWindowStore<>(new
                    StateStoreProviderStub(false), QueryableStoreTypes.windowStore(), "foo");
            WindowStoreIterator<object> windowStoreIterator = store.fetch("key", ofEpochMilli(1), ofEpochMilli(10));
            assertThrows(NoSuchElementException, windowStoreIterator::next);
        }

        [Xunit.Fact]
        public void ShouldFetchKeyRangeAcrossStores()
        {
            ReadOnlyWindowStoreStub<string, string> secondUnderlying = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
            stubProviderTwo.addStore(storeName, secondUnderlying);
            underlyingWindowStore.put("a", "a", 0L);
            secondUnderlying.put("b", "b", 10L);
            List<KeyValuePair<Windowed<string>, string>> results = StreamsTestUtils.toList(windowStore.fetch("a", "b", ofEpochMilli(0), ofEpochMilli(10)));
            Assert.Equal(results, (Array.asList(
                    KeyValuePair.Create(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
                    KeyValuePair.Create(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
        }

        [Xunit.Fact]
        public void ShouldFetchKeyValueAcrossStores()
        {
            ReadOnlyWindowStoreStub<string, string> secondUnderlyingWindowStore = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
            stubProviderTwo.addStore(storeName, secondUnderlyingWindowStore);
            underlyingWindowStore.put("a", "a", 0L);
            secondUnderlyingWindowStore.put("b", "b", 10L);
            Assert.Equal(windowStore.fetch("a", 0L), ("a"));
            Assert.Equal(windowStore.fetch("b", 10L), ("b"));
            Assert.Equal(windowStore.fetch("c", 10L), (null));
            Assert.Equal(windowStore.fetch("a", 10L), (null));
        }


        [Xunit.Fact]
        public void ShouldGetAllAcrossStores()
        {
            ReadOnlyWindowStoreStub<string, string> secondUnderlying = new
                    ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
            stubProviderTwo.addStore(storeName, secondUnderlying);
            underlyingWindowStore.put("a", "a", 0L);
            secondUnderlying.put("b", "b", 10L);
            List<KeyValuePair<Windowed<string>, string>> results = StreamsTestUtils.toList(windowStore.all());
            Assert.Equal(results, (Array.asList(
                    KeyValuePair.Create(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
                    KeyValuePair.Create(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
        }

        [Xunit.Fact]
        public void ShouldFetchAllAcrossStores()
        {
            ReadOnlyWindowStoreStub<string, string> secondUnderlying = new
                    ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
            stubProviderTwo.addStore(storeName, secondUnderlying);
            underlyingWindowStore.put("a", "a", 0L);
            secondUnderlying.put("b", "b", 10L);
            List<KeyValuePair<Windowed<string>, string>> results = StreamsTestUtils.toList(windowStore.fetchAll(ofEpochMilli(0), ofEpochMilli(10)));
            Assert.Equal(results, (Array.asList(
                    KeyValuePair.Create(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
                    KeyValuePair.Create(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
        }

        [Xunit.Fact]// (expected = NullPointerException)
        public void ShouldThrowNPEIfKeyIsNull()
        {
            windowStore.fetch(null, ofEpochMilli(0), ofEpochMilli(0));
        }

        [Xunit.Fact]// (expected = NullPointerException)
        public void ShouldThrowNPEIfFromKeyIsNull()
        {
            windowStore.fetch(null, "a", ofEpochMilli(0), ofEpochMilli(0));
        }

        [Xunit.Fact]// (expected = NullPointerException)
        public void ShouldThrowNPEIfToKeyIsNull()
        {
            windowStore.fetch("a", null, ofEpochMilli(0), ofEpochMilli(0));
        }

    }
}
/*






*

*





*/


























