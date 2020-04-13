//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.ReadOnly;
//using Kafka.Streams.State.TimeStamped;
//using Kafka.Streams.State.Windowed;
//
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

























//    public class ReadOnlyWindowStoreFacadeTest
//    {

//        private ITimestampedWindowStore<string, string> mockedWindowTimestampStore;

//        private IWindowStoreIterator<IValueAndTimestamp<string>> mockedWindowTimestampIterator;

//        private IKeyValueIterator<IWindowed<string>, IValueAndTimestamp<string>> mockedKeyValueWindowTimestampIterator;

//        private ReadOnlyWindowStoreFacade<string, string> readOnlyWindowStoreFacade;


//        public void Setup()
//        {
//            readOnlyWindowStoreFacade = new ReadOnlyWindowStoreFacade<string, string>(mockedWindowTimestampStore);
//        }

//        [Fact]
//        public void ShouldReturnPlainKeyValuePairsOnSingleKeyFetch()
//        {
//            expect(mockedWindowTimestampStore.Fetch("key1", 21L))
//                .andReturn(ValueAndTimestamp.Make("value1", 42L));
//            expect(mockedWindowTimestampStore.Fetch("unknownKey", 21L))
//                .andReturn(null);
//            replay(mockedWindowTimestampStore);

//            Assert.Equal("value1", readOnlyWindowStoreFacade.Fetch("key1", 21L));
//            Assert.Null(readOnlyWindowStoreFacade.Fetch("unknownKey", 21L));

//            verify(mockedWindowTimestampStore);
//        }

//        [Fact]
//        public void ShouldReturnPlainKeyValuePairsOnSingleKeyFetchLongParameters()
//        {
//            expect(mockedWindowTimestampIterator.MoveNext())
//                .andReturn(KeyValuePair.Create(21L, ValueAndTimestamp.Make("value1", 22L)))
//                .andReturn(KeyValuePair.Create(42L, ValueAndTimestamp.Make("value2", 23L)));
//            expect(mockedWindowTimestampStore.Fetch("key1", 21L, 42L))
//                .andReturn(mockedWindowTimestampIterator);
//            replay(mockedWindowTimestampIterator, mockedWindowTimestampStore);

//            IWindowStoreIterator<string> iterator =
//                readOnlyWindowStoreFacade.Fetch("key1", 21L, 42L);

//            Assert.Equal(iterator.Current, KeyValuePair.Create(21L, "value1")); iterator.MoveNext();
//            Assert.Equal(iterator.Current, KeyValuePair.Create(42L, "value2")); iterator.MoveNext();
//            verify(mockedWindowTimestampIterator, mockedWindowTimestampStore);
//        }

//        [Fact]
//        public void ShouldReturnPlainKeyValuePairsOnSingleKeyFetchInstantParameters()
//        {
//            expect(mockedWindowTimestampIterator.MoveNext())
//                .andReturn(KeyValuePair.Create(21L, ValueAndTimestamp.Make("value1", 22L)))
//                .andReturn(KeyValuePair.Create(42L, ValueAndTimestamp.Make("value2", 23L)));
//            expect(mockedWindowTimestampStore.Fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
//                .andReturn(mockedWindowTimestampIterator);
//            replay(mockedWindowTimestampIterator, mockedWindowTimestampStore);

//            IWindowStoreIterator<string> iterator =
//                readOnlyWindowStoreFacade.Fetch("key1", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

//            Assert.Equal(iterator.Current, KeyValuePair.Create(21L, "value1")); iterator.MoveNext();
//            Assert.Equal(iterator.Current, KeyValuePair.Create(42L, "value2")); iterator.MoveNext();
//            verify(mockedWindowTimestampIterator, mockedWindowTimestampStore);
//        }

//        [Fact]
//        public void ShouldReturnPlainKeyValuePairsOnRangeFetchLongParameters()
//        {
//            expect(mockedKeyValueWindowTimestampIterator.MoveNext())
//                .andReturn(KeyValuePair.Create(
//                    new Windowed2<>("key1", new TimeWindow(21L, 22L)),
//                    ValueAndTimestamp.Make("value1", 22L)))
//                .andReturn(KeyValuePair.Create(
//                    new Windowed2<>("key2", new TimeWindow(42L, 43L)),
//                    ValueAndTimestamp.Make("value2", 100L)));
//            expect(mockedWindowTimestampStore.Fetch("key1", "key2", 21L, 42L))
//                .andReturn(mockedKeyValueWindowTimestampIterator);
//            replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

//            IKeyValueIterator<IWindowed<string>, string> iterator =
//                readOnlyWindowStoreFacade.Fetch("key1", "key2", 21L, 42L);

//            Assert.Equal(iterator.MoveNext(), KeyValuePair.Create(new Windowed2<>("key1", new TimeWindow(21L, 22L)), "value1"));
//            Assert.Equal(iterator.MoveNext(), KeyValuePair.Create(new Windowed2<>("key2", new TimeWindow(42L, 43L)), "value2"));
//            verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
//        }

//        [Fact]
//        public void ShouldReturnPlainKeyValuePairsOnRangeFetchInstantParameters()
//        {
//            expect(mockedKeyValueWindowTimestampIterator.MoveNext())
//                .andReturn(KeyValuePair.Create(
//                    new Windowed2<>("key1", new TimeWindow(21L, 22L)),
//                    ValueAndTimestamp.Make("value1", 22L)))
//                .andReturn(KeyValuePair.Create(
//                    new Windowed2<>("key2", new TimeWindow(42L, 43L)),
//                    ValueAndTimestamp.Make("value2", 100L)));
//            expect(mockedWindowTimestampStore.Fetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
//                .andReturn(mockedKeyValueWindowTimestampIterator);
//            replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

//            IKeyValueIterator<IWindowed<string>, string> iterator =
//                readOnlyWindowStoreFacade.Fetch("key1", "key2", Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

//            Assert.Equal(iterator.MoveNext(), KeyValuePair.Create(new Windowed2<>("key1", new TimeWindow(21L, 22L)), "value1"));
//            Assert.Equal(iterator.MoveNext(), KeyValuePair.Create(new Windowed2<>("key2", new TimeWindow(42L, 43L)), "value2"));
//            verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
//        }

//        [Fact]
//        public void ShouldReturnPlainKeyValuePairsOnFetchAllLongParameters()
//        {
//            expect(mockedKeyValueWindowTimestampIterator.MoveNext())
//                .andReturn(KeyValuePair.Create(
//                    new Windowed2<string>("key1", new TimeWindow(21L, 22L)),
//                    ValueAndTimestamp.Make("value1", 22L)))
//                .andReturn(KeyValuePair.Create(
//                    new Windowed2<string>("key2", new TimeWindow(42L, 43L)),
//                    ValueAndTimestamp.Make("value2", 100L)));
//            expect(mockedWindowTimestampStore.FetchAll(21L, 42L))
//                .andReturn(mockedKeyValueWindowTimestampIterator);
//            replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

//            IKeyValueIterator<IWindowed<string>, string> iterator =
//                readOnlyWindowStoreFacade.FetchAll(21L, 42L);

//            Assert.Equal(iterator.MoveNext(), KeyValuePair.Create(new Windowed2<>("key1", new TimeWindow(21L, 22L)), "value1"));
//            Assert.Equal(iterator.MoveNext(), KeyValuePair.Create(new Windowed2<>("key2", new TimeWindow(42L, 43L)), "value2"));
//            verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
//        }

//        [Fact]
//        public void ShouldReturnPlainKeyValuePairsOnFetchAllInstantParameters()
//        {
//            expect(mockedKeyValueWindowTimestampIterator.MoveNext())
//                .andReturn(KeyValuePair.Create(
//                    new Windowed2<>("key1", new TimeWindow(21L, 22L)),
//                    ValueAndTimestamp.Make("value1", 22L)))
//                .andReturn(KeyValuePair.Create(
//                    new Windowed2<>("key2", new TimeWindow(42L, 43L)),
//                    ValueAndTimestamp.Make("value2", 100L)));
//            expect(mockedWindowTimestampStore.FetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L)))
//                .andReturn(mockedKeyValueWindowTimestampIterator);
//            replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

//            IKeyValueIterator<IWindowed<string>, string> iterator =
//                readOnlyWindowStoreFacade.FetchAll(Instant.ofEpochMilli(21L), Instant.ofEpochMilli(42L));

//            Assert.Equal(iterator.MoveNext(), KeyValuePair.Create(new Windowed2<>("key1", new TimeWindow(21L, 22L)), "value1"));
//            Assert.Equal(iterator.MoveNext(), KeyValuePair.Create(new Windowed2<>("key2", new TimeWindow(42L, 43L)), "value2"));
//            verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
//        }

//        [Fact]
//        public void ShouldReturnPlainKeyValuePairsOnAll()
//        {
//            expect(mockedKeyValueWindowTimestampIterator.MoveNext())
//                .andReturn(KeyValuePair.Create(
//                    new Windowed2<>("key1", new TimeWindow(21L, 22L)),
//                    ValueAndTimestamp.Make("value1", 22L)))
//                .andReturn(KeyValuePair.Create(
//                    new Windowed2<>("key2", new TimeWindow(42L, 43L)),
//                    ValueAndTimestamp.Make("value2", 100L)));
//            expect(mockedWindowTimestampStore.All()).andReturn(mockedKeyValueWindowTimestampIterator);
//            replay(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);

//            IKeyValueIterator<IWindowed<string>, string> iterator = readOnlyWindowStoreFacade.All();

//            Assert.Equal(iterator.MoveNext(), KeyValuePair.Create(new Windowed2<>("key1", new TimeWindow(21L, 22L)), "value1"));
//            Assert.Equal(iterator.MoveNext(), KeyValuePair.Create(new Windowed2<>("key2", new TimeWindow(42L, 43L)), "value2"));
//            verify(mockedKeyValueWindowTimestampIterator, mockedWindowTimestampStore);
//        }
//    }
//}
