//using Kafka.Streams.KStream;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State.Internals;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */
























//    public class SegmentIteratorTest
//    {

//        private KeyValueSegment segmentOne = new KeyValueSegment("one", "one", 0);
//        private KeyValueSegment segmentTwo = new KeyValueSegment("two", "window", 1);
//        private HasNextCondition hasNextCondition = Iterator::hasNext;

//        private SegmentIterator<KeyValueSegment> iterator = null;


//        public void Before()
//        {
//            InternalMockProcessorContext context = new InternalMockProcessorContext(
//                    TestUtils.GetTempDirectory(),
//                    Serdes.String(),
//                    Serdes.String(),
//                    new NoOpRecordCollector(),
//                    new ThreadCache(
//                        new LogContext("testCache "),
//                        0,
//                        new MockStreamsMetrics(new Metrics())));
//            segmentOne.OpenDb(context);
//            segmentTwo.OpenDb(context);
//            segmentOne.Put(Bytes.Wrap("a".getBytes()), "1".getBytes());
//            segmentOne.Put(Bytes.Wrap("b".getBytes()), "2".getBytes());
//            segmentTwo.Put(Bytes.Wrap("c".getBytes()), "3".getBytes());
//            segmentTwo.Put(Bytes.Wrap("d".getBytes()), "4".getBytes());
//        }


//        public void CloseSegments()
//        {
//            if (iterator != null)
//            {
//                iterator.Close();
//                iterator = null;
//            }
//            segmentOne.Close();
//            segmentTwo.Close();
//        }

//        [Fact]
//        public void ShouldIterateOverAllSegments()
//        {
//            iterator = new SegmentIterator<>(
//                Array.asList(segmentOne, segmentTwo).iterator(),
//                hasNextCondition,
//                Bytes.Wrap("a".getBytes()),
//                Bytes.Wrap("z".getBytes()));

//            Assert.True(iterator.HasNext());
//            Assert.Equal("a", new string(iterator.PeekNextKey().Get()));
//            Assert.Equal(KeyValuePair.Create("a", "1"), toStringKeyValue(iterator.MoveNext()));

//            Assert.True(iterator.HasNext());
//            Assert.Equal("b", new string(iterator.PeekNextKey().Get()));
//            Assert.Equal(KeyValuePair.Create("b", "2"), toStringKeyValue(iterator.MoveNext()));

//            Assert.True(iterator.HasNext());
//            Assert.Equal("c", new string(iterator.PeekNextKey().Get()));
//            Assert.Equal(KeyValuePair.Create("c", "3"), toStringKeyValue(iterator.MoveNext()));

//            Assert.True(iterator.HasNext());
//            Assert.Equal("d", new string(iterator.PeekNextKey().Get()));
//            Assert.Equal(KeyValuePair.Create("d", "4"), toStringKeyValue(iterator.MoveNext()));

//            Assert.False(iterator.HasNext());
//        }

//        [Fact]
//        public void ShouldNotThrowExceptionOnHasNextWhenStoreClosed()
//        {
//            iterator = new SegmentIterator<>(
//                Collections.singletonList(segmentOne).iterator(),
//                hasNextCondition,
//                Bytes.Wrap("a".getBytes()),
//                Bytes.Wrap("z".getBytes()));

//            iterator.currentIterator = segmentOne.All();
//            segmentOne.Close();
//            Assert.False(iterator.HasNext());
//        }

//        [Fact]
//        public void ShouldOnlyIterateOverSegmentsInRange()
//        {
//            iterator = new SegmentIterator<>(
//                Array.asList(segmentOne, segmentTwo).iterator(),
//                hasNextCondition,
//                Bytes.Wrap("a".getBytes()),
//                Bytes.Wrap("b".getBytes()));

//            Assert.True(iterator.HasNext());
//            Assert.Equal("a", new string(iterator.PeekNextKey().Get()));
//            Assert.Equal(KeyValuePair.Create("a", "1"), toStringKeyValue(iterator.MoveNext()));

//            Assert.True(iterator.HasNext());
//            Assert.Equal("b", new string(iterator.PeekNextKey().Get()));
//            Assert.Equal(KeyValuePair.Create("b", "2"), toStringKeyValue(iterator.MoveNext()));

//            Assert.False(iterator.HasNext());
//        }

//        [Fact]// (expected = NoSuchElementException)
//        public void ShouldThrowNoSuchElementOnPeekNextKeyIfNoNext()
//        {
//            iterator = new SegmentIterator<>(
//                Array.asList(segmentOne, segmentTwo).iterator(),
//                hasNextCondition,
//                Bytes.Wrap("f".getBytes()),
//                Bytes.Wrap("h".getBytes()));

//            iterator.PeekNextKey();
//        }

//        [Fact]// (expected = NoSuchElementException)
//        public void ShouldThrowNoSuchElementOnNextIfNoNext()
//        {
//            iterator = new SegmentIterator<>(
//                Array.asList(segmentOne, segmentTwo).iterator(),
//                hasNextCondition,
//                Bytes.Wrap("f".getBytes()),
//                Bytes.Wrap("h".getBytes()));

//            iterator.MoveNext();
//        }

//        private KeyValuePair<string, string> ToStringKeyValue(KeyValuePair<Bytes, byte[]> binaryKv)
//        {
//            return KeyValuePair.Create("", ""); // KeyValuePair.Create(new string(binaryKv.Key.Get()), new string(binaryKv.Value));
//        }
//    }
//}
///*






//*

//*





//*/
























