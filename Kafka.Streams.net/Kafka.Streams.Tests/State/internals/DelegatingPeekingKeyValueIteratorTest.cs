//using Kafka.Streams.State.Internals;
//using Kafka.Streams.State.KeyValues;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */













//    public class DelegatingPeekingKeyValueIteratorTest
//    {

//        private readonly string name = "name";
//        private IKeyValueStore<string, string> store;


//        public void SetUp()
//        {
//            store = new GenericInMemoryKeyValueStore<>(name);
//        }

//        [Fact]
//        public void ShouldPeekNextKey()
//        {
//            store.Add("A", "A");
//            DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
//            Assert.Equal("A", peekingIterator.peekNextKey());
//            Assert.Equal("A", peekingIterator.peekNextKey());
//            Assert.True(peekingIterator.hasNext());
//            peekingIterator.close();
//        }

//        [Fact]
//        public void ShouldPeekNext()
//        {
//            store.Add("A", "A");
//            DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
//            Assert.Equal(KeyValuePair.Create("A", "A"), peekingIterator.peekNext());
//            Assert.Equal(KeyValuePair.Create("A", "A"), peekingIterator.peekNext());
//            Assert.True(peekingIterator.hasNext());
//            peekingIterator.close();
//        }

//        [Fact]
//        public void ShouldPeekAndIterate()
//        {
//            string[] kvs = { "a", "b", "c", "d", "e", "f" };
//            foreach (string kv in kvs)
//            {
//                store.put(kv, kv);
//            }

//            DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
//            int index = 0;
//            while (peekingIterator.hasNext())
//            {
//                string peekNext = peekingIterator.peekNextKey();
//                string key = peekingIterator.MoveNext().key;
//                Assert.Equal(kvs[index], peekNext);
//                Assert.Equal(kvs[index], key);
//                index++;
//            }
//            Assert.Equal(kvs.Length, index);
//            peekingIterator.close();
//        }

//        [Fact]// (expected = NoSuchElementException)
//        public void ShouldThrowNoSuchElementWhenNoMoreItemsLeftAndNextCalled()
//        {
//            DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
//            peekingIterator.MoveNext();
//            peekingIterator.close();
//        }

//        [Fact]// (expected = NoSuchElementException)
//        public void ShouldThrowNoSuchElementWhenNoMoreItemsLeftAndPeekNextCalled()
//        {
//            DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(name, store.all());
//            peekingIterator.peekNextKey();
//            peekingIterator.close();
//        }


//    }
//}
///*






//*

//*





//*/













