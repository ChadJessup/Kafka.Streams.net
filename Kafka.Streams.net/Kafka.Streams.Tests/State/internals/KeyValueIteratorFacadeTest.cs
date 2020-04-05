//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class KeyValueIteratorFacadeTest
//    {

//        private IKeyValueIterator<string, ValueAndTimestamp<string>> mockedKeyValueIterator;

//        private KeyValueIteratorFacade<string, string> keyValueIteratorFacade;


//        public void Setup()
//        {
//            keyValueIteratorFacade = new KeyValueIteratorFacade<>(mockedKeyValueIterator);
//        }

//        [Xunit.Fact]
//        public void ShouldForwardHasNext()
//        {
//            expect(mockedKeyValueIterator.hasNext()).andReturn(true).andReturn(false);
//            replay(mockedKeyValueIterator);

//            Assert.True(keyValueIteratorFacade.hasNext());
//            Assert.False(keyValueIteratorFacade.hasNext());
//            verify(mockedKeyValueIterator);
//        }

//        [Xunit.Fact]
//        public void ShouldForwardPeekNextKey()
//        {
//            expect(mockedKeyValueIterator.peekNextKey()).andReturn("key");
//            replay(mockedKeyValueIterator);

//            Assert.Equal(keyValueIteratorFacade.peekNextKey(), "key");
//            verify(mockedKeyValueIterator);
//        }

//        [Xunit.Fact]
//        public void ShouldReturnPlainKeyValuePairOnGet()
//        {
//            expect(mockedKeyValueIterator.MoveNext()).andReturn(
//                KeyValuePair.Create("key", ValueAndTimestamp.Make("value", 42L)));
//            replay(mockedKeyValueIterator);

//            Assert.Equal(keyValueIteratorFacade.MoveNext(), KeyValuePair.Create("key", "value"));
//            verify(mockedKeyValueIterator);
//        }

//        [Xunit.Fact]
//        public void ShouldCloseInnerIterator()
//        {
//            mockedKeyValueIterator.close();
//            expectLastCall();
//            replay(mockedKeyValueIterator);

//            keyValueIteratorFacade.close();
//            verify(mockedKeyValueIterator);
//        }
//    }
//}
