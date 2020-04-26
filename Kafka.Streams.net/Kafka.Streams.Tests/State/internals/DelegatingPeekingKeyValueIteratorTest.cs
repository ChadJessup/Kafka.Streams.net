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

//        private readonly string Name = "Name";
//        private IKeyValueStore<string, string> store;


//        public void SetUp()
//        {
//            store = new GenericInMemoryKeyValueStore<>(Name);
//        }

//        [Fact]
//        public void ShouldPeekNextKey()
//        {
//            store.Add("A", "A");
//            DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(Name, store.All());
//            Assert.Equal("A", peekingIterator.PeekNextKey());
//            Assert.Equal("A", peekingIterator.PeekNextKey());
//            Assert.True(peekingIterator.MoveNext());
//            peekingIterator.Close();
//        }

//        [Fact]
//        public void ShouldPeekNext()
//        {
//            store.Add("A", "A");
//            DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(Name, store.All());
//            Assert.Equal(KeyValuePair.Create("A", "A"), peekingIterator.PeekNext());
//            Assert.Equal(KeyValuePair.Create("A", "A"), peekingIterator.PeekNext());
//            Assert.True(peekingIterator.MoveNext());
//            peekingIterator.Close();
//        }

//        [Fact]
//        public void ShouldPeekAndIterate()
//        {
//            string[] kvs = { "a", "b", "c", "d", "e", "f" };
//            foreach (string kv in kvs)
//            {
//                store.Put(kv, kv);
//            }

//            DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(Name, store.All());
//            int index = 0;
//            while (peekingIterator.MoveNext())
//            {
//                string peekNext = peekingIterator.PeekNextKey();
//                string key = peekingIterator.MoveNext().Key;
//                Assert.Equal(kvs[index], peekNext);
//                Assert.Equal(kvs[index], key);
//                index++;
//            }
//            Assert.Equal(kvs.Length, index);
//            peekingIterator.Close();
//        }

//        [Fact]// (expected = NoSuchElementException)
//        public void ShouldThrowNoSuchElementWhenNoMoreItemsLeftAndNextCalled()
//        {
//            DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(Name, store.All());
//            peekingIterator.MoveNext();
//            peekingIterator.Close();
//        }

//        [Fact]// (expected = NoSuchElementException)
//        public void ShouldThrowNoSuchElementWhenNoMoreItemsLeftAndPeekNextCalled()
//        {
//            DelegatingPeekingKeyValueIterator<string, string> peekingIterator = new DelegatingPeekingKeyValueIterator<>(Name, store.All());
//            peekingIterator.PeekNextKey();
//            peekingIterator.Close();
//        }


//    }
//}
///*






//*

//*





//*/













