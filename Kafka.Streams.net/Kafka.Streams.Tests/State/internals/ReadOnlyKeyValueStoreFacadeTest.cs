//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.ReadOnly;
//using Kafka.Streams.State.TimeStamped;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class ReadOnlyKeyValueStoreFacadeTest
//    {

//        private ITimestampedKeyValueStore<string, string> mockedKeyValueTimestampStore;

//        private IKeyValueIterator<string, ValueAndTimestamp<string>> mockedKeyValueTimestampIterator;

//        private ReadOnlyKeyValueStoreFacade<string, string> readOnlyKeyValueStoreFacade;


//        public void Setup()
//        {
//            readOnlyKeyValueStoreFacade = new ReadOnlyKeyValueStoreFacade<>(mockedKeyValueTimestampStore);
//        }

//        [Xunit.Fact]
//        public void ShouldReturnPlainValueOnGet()
//        {
//            expect(mockedKeyValueTimestampStore.Get("key"))
//                .andReturn(ValueAndTimestamp.Make("value", 42L));
//            expect(mockedKeyValueTimestampStore.Get("unknownKey"))
//                .andReturn(null);
//            replay(mockedKeyValueTimestampStore);

//            Assert.Equal(readOnlyKeyValueStoreFacade.Get("key"), "value");
//            Assert.Null(readOnlyKeyValueStoreFacade.Get("unknownKey"));
//            verify(mockedKeyValueTimestampStore);
//        }

//        [Xunit.Fact]
//        public void ShouldReturnPlainKeyValuePairsForRangeIterator()
//        {
//            expect(mockedKeyValueTimestampIterator.MoveNext())
//                .andReturn(KeyValuePair.Create("key1", ValueAndTimestamp.Make("value1", 21L)))
//                .andReturn(KeyValuePair.Create("key2", ValueAndTimestamp.Make("value2", 42L)));
//            expect(mockedKeyValueTimestampStore.Range("key1", "key2")).andReturn(mockedKeyValueTimestampIterator);
//            replay(mockedKeyValueTimestampIterator, mockedKeyValueTimestampStore);

//            IKeyValueIterator<string, string> iterator = readOnlyKeyValueStoreFacade.Range("key1", "key2");
//            Assert.Equal(iterator.Current, KeyValuePair.Create("key1", "value1")); iterator.MoveNext();
//            Assert.Equal(iterator.Current, KeyValuePair.Create("key2", "value2")); iterator.MoveNext();
//            verify(mockedKeyValueTimestampIterator, mockedKeyValueTimestampStore);
//        }

//        [Xunit.Fact]
//        public void ShouldReturnPlainKeyValuePairsForAllIterator()
//        {
//            expect(mockedKeyValueTimestampIterator.MoveNext())
//                .andReturn(KeyValuePair.Create("key1", ValueAndTimestamp.Make("value1", 21L)))
//                .andReturn(KeyValuePair.Create("key2", ValueAndTimestamp.Make("value2", 42L)));
//            expect(mockedKeyValueTimestampStore.All()).andReturn(mockedKeyValueTimestampIterator);
//            replay(mockedKeyValueTimestampIterator, mockedKeyValueTimestampStore);

//            IKeyValueIterator<string, string> iterator = readOnlyKeyValueStoreFacade.All();
//            Assert.Equal(iterator.Current, KeyValuePair.Create("key1", "value1")); iterator.MoveNext();
//            Assert.Equal(iterator.Current, KeyValuePair.Create("key2", "value2")); iterator.MoveNext();
//            verify(mockedKeyValueTimestampIterator, mockedKeyValueTimestampStore);
//        }

//        [Xunit.Fact]
//        public void ShouldForwardApproximateNumEntries()
//        {
//            expect(mockedKeyValueTimestampStore.approximateNumEntries).andReturn(42L);
//            replay(mockedKeyValueTimestampStore);

//            Assert.Equal(readOnlyKeyValueStoreFacade.approximateNumEntries, 42L);
//            verify(mockedKeyValueTimestampStore);
//        }
//    }
//}
///*






//*

//*





//*/




















