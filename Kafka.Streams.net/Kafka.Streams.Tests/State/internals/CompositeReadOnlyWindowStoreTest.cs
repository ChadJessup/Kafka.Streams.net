//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State.Internals;
//using Kafka.Streams.State.Queryable;
//using Kafka.Streams.State.Windowed;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class CompositeReadOnlyWindowStoreTest
//    {
//        private const long WINDOW_SIZE = 30_000;

//        private readonly string storeName = "window-store";
//        private StateStoreProviderStub stubProviderOne;
//        private StateStoreProviderStub stubProviderTwo;
//        private CompositeReadOnlyWindowStore<string, string> windowStore;
//        private ReadOnlyWindowStoreStub<string, string> underlyingWindowStore;
//        private ReadOnlyWindowStoreStub<string, string> otherUnderlyingStore;


//        public void Before()
//        {
//            stubProviderOne = new StateStoreProviderStub(false);
//            stubProviderTwo = new StateStoreProviderStub(false);
//            underlyingWindowStore = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
//            stubProviderOne.addStore(storeName, underlyingWindowStore);

//            otherUnderlyingStore = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
//            stubProviderOne.addStore("other-window-store", otherUnderlyingStore);


//            windowStore = new CompositeReadOnlyWindowStore<>(
//                new WrappingStoreProvider(Array.< StateStoreProvider > Arrays.asList(stubProviderOne, stubProviderTwo)),
//                    QueryableStoreTypes.< string, string > windowStore(),
//                    storeName);
//        }

//        [Fact]
//        public void ShouldFetchValuesFromWindowStore()
//        {
//            underlyingWindowStore.Put("my-key", "my-value", 0L);
//            underlyingWindowStore.Put("my-key", "my-later-value", 10L);

//            IWindowStoreIterator<string> iterator = windowStore.Fetch("my-key", TimeSpan.FromMilliseconds(0L), TimeSpan.FromMilliseconds(25L));
//            List<KeyValuePair<long, string>> results = StreamsTestUtils.toList(iterator);

//            Assert.Equal(asList(new KeyValuePair<long, string>(0L, "my-value"),
//                                new KeyValuePair<long, string>(10L, "my-later-value")),
//                         results);
//        }

//        [Fact]
//        public void ShouldReturnEmptyIteratorIfNoData()
//        {
//            IWindowStoreIterator<string> iterator = windowStore.Fetch("my-key", TimeSpan.FromMilliseconds(0L), TimeSpan.FromMilliseconds(25L));
//            Assert.Equal(false, iterator.MoveNext());
//        }

//        [Fact]
//        public void ShouldFindValueForKeyWhenMultiStores()
//        {
//            ReadOnlyWindowStoreStub<string, string> secondUnderlying = new
//                ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
//            stubProviderTwo.addStore(storeName, secondUnderlying);

//            underlyingWindowStore.Put("key-one", "value-one", 0L);
//            secondUnderlying.Put("key-two", "value-two", 10L);

//            List<KeyValuePair<long, string>> keyOneResults = StreamsTestUtils.toList(windowStore.Fetch("key-one", TimeSpan.FromMilliseconds(0L),
//                                                                                                         TimeSpan.FromMilliseconds(1L)));
//            List<KeyValuePair<long, string>> keyTwoResults = StreamsTestUtils.toList(windowStore.Fetch("key-two", TimeSpan.FromMilliseconds(10L),
//                                                                                                         TimeSpan.FromMilliseconds(11L)));

//            Assert.Equal(Collections.singletonList(KeyValuePair.Create(0L, "value-one")), keyOneResults);
//            Assert.Equal(Collections.singletonList(KeyValuePair.Create(10L, "value-two")), keyTwoResults);
//        }

//        [Fact]
//        public void ShouldNotGetValuesFromOtherStores()
//        {
//            otherUnderlyingStore.Put("some-key", "some-value", 0L);
//            underlyingWindowStore.Put("some-key", "my-value", 1L);

//            List<KeyValuePair<long, string>> results = StreamsTestUtils.toList(windowStore.Fetch("some-key", TimeSpan.FromMilliseconds(0L), TimeSpan.FromMilliseconds(2L)));
//            Assert.Equal(Collections.singletonList(KeyValuePair.Create(1L, "my-value")), results);
//        }

//        [Fact]// (expected = InvalidStateStoreException)
//        public void ShouldThrowInvalidStateStoreExceptionOnRebalance()
//        {
//            CompositeReadOnlyWindowStore<object, object> store = new CompositeReadOnlyWindowStore<>(new StateStoreProviderStub(true), QueryableStoreTypes.windowStore(), "foo");
//            store.Fetch("key", TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(10));
//        }

//        [Fact]
//        public void ShouldThrowInvalidStateStoreExceptionIfFetchThrows()
//        {
//            underlyingWindowStore.setOpen(false);
//            CompositeReadOnlyWindowStore<object, object> store =
//                    new CompositeReadOnlyWindowStore<>(stubProviderOne, QueryableStoreTypes.windowStore(), "window-store");
//            try
//            {
//                store.Fetch("key", TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(10));
//                Assert.True(false, "InvalidStateStoreException was expected");
//            }
//            catch (InvalidStateStoreException e)
//            {
//                Assert.Equal("State store is not available anymore and may have been migrated to another instance; " +
//                        "please re-discover its location from the state metadata.", e.ToString());
//            }
//        }

//        [Fact]
//        public void EmptyIteratorAlwaysReturnsFalse()
//        {
//            CompositeReadOnlyWindowStore<object, object> store = new CompositeReadOnlyWindowStore<>(new
//                    StateStoreProviderStub(false), QueryableStoreTypes.windowStore(), "foo");
//            IWindowStoreIterator<object> windowStoreIterator = store.Fetch("key", TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(10));

//            Assert.False(windowStoreIterator.MoveNext());
//        }

//        [Fact]
//        public void EmptyIteratorPeekNextKeyShouldThrowNoSuchElementException()
//        {
//            CompositeReadOnlyWindowStore<object, object> store = new CompositeReadOnlyWindowStore<>(new
//                    StateStoreProviderStub(false), QueryableStoreTypes.windowStore(), "foo");
//            IWindowStoreIterator<object> windowStoreIterator = store.Fetch("key", TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(10));
//            Assert.Throws<NotImplementedException>(() => windowStoreIterator::peekNextKey);
//        }

//        [Fact]
//        public void EmptyIteratorNextShouldThrowNoSuchElementException()
//        {
//            CompositeReadOnlyWindowStore<object, object> store = new CompositeReadOnlyWindowStore<>(new
//                    StateStoreProviderStub(false), QueryableStoreTypes.windowStore(), "foo");
//            IWindowStoreIterator<object> windowStoreIterator = store.Fetch("key", TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(10));
//            Assert.Throws(NoSuchElementException, windowStoreIterator::next);
//        }

//        [Fact]
//        public void ShouldFetchKeyRangeAcrossStores()
//        {
//            ReadOnlyWindowStoreStub<string, string> secondUnderlying = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
//            stubProviderTwo.addStore(storeName, secondUnderlying);
//            underlyingWindowStore.Put("a", "a", 0L);
//            secondUnderlying.Put("b", "b", 10L);
//            List<KeyValuePair<IWindowed<string>, string>> results = StreamsTestUtils.toList(windowStore.Fetch("a", "b", TimeSpan.FromMilliseconds(0), TimeSpan.FromMilliseconds(10)));
//            Assert.Equal(results, (Arrays.asList(
//                    KeyValuePair.Create(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
//                    KeyValuePair.Create(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
//        }

//        [Fact]
//        public void ShouldFetchKeyValueAcrossStores()
//        {
//            ReadOnlyWindowStoreStub<string, string> secondUnderlyingWindowStore = new ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
//            stubProviderTwo.addStore(storeName, secondUnderlyingWindowStore);
//            underlyingWindowStore.Put("a", "a", 0L);
//            secondUnderlyingWindowStore.Put("b", "b", 10L);
//            Assert.Equal(windowStore.Fetch("a", 0L), ("a"));
//            Assert.Equal(windowStore.Fetch("b", 10L), ("b"));
//            Assert.Equal(windowStore.Fetch("c", 10L), (null));
//            Assert.Equal(windowStore.Fetch("a", 10L), (null));
//        }


//        [Fact]
//        public void ShouldGetAllAcrossStores()
//        {
//            ReadOnlyWindowStoreStub<string, string> secondUnderlying = new
//                    ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
//            stubProviderTwo.addStore(storeName, secondUnderlying);
//            underlyingWindowStore.Put("a", "a", 0L);
//            secondUnderlying.Put("b", "b", 10L);
//            List<KeyValuePair<IWindowed<string>, string>> results = StreamsTestUtils.toList(windowStore.All());
//            Assert.Equal(results, (Arrays.asList(
//                    KeyValuePair.Create(new Windowed<>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
//                    KeyValuePair.Create(new Windowed<>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
//        }

//        [Fact]
//        public void ShouldFetchAllAcrossStores()
//        {
//            ReadOnlyWindowStoreStub<string, string> secondUnderlying = new
//                    ReadOnlyWindowStoreStub<>(WINDOW_SIZE);
//            stubProviderTwo.addStore(storeName, secondUnderlying);
//            underlyingWindowStore.Put("a", "a", 0L);
//            secondUnderlying.Put("b", "b", 10L);
//            List<KeyValuePair<IWindowed<string>, string>> results = StreamsTestUtils.toList(windowStore.FetchAll(TimeSpan.FromMilliseconds(0), TimeSpan.FromMilliseconds(10)));
//            Assert.Equal(results, (Arrays.asList(
//                    KeyValuePair.Create(new Windowed<string>("a", new TimeWindow(0, WINDOW_SIZE)), "a"),
//                    KeyValuePair.Create(new Windowed<string>("b", new TimeWindow(10, 10 + WINDOW_SIZE)), "b"))));
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowNPEIfKeyIsNull()
//        {
//            windowStore.Fetch(null, TimeSpan.FromMilliseconds(0), TimeSpan.FromMilliseconds(0));
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowNPEIfFromKeyIsNull()
//        {
//            windowStore.Fetch(null, "a", TimeSpan.FromMilliseconds(0), TimeSpan.FromMilliseconds(0));
//        }

//        [Fact]// (expected = NullReferenceException)
//        public void ShouldThrowNPEIfToKeyIsNull()
//        {
//            windowStore.Fetch("a", null, TimeSpan.FromMilliseconds(0), TimeSpan.FromMilliseconds(0));
//        }

//    }
//}
