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
//            segmentOne.put(Bytes.Wrap("a".getBytes()), "1".getBytes());
//            segmentOne.put(Bytes.Wrap("b".getBytes()), "2".getBytes());
//            segmentTwo.put(Bytes.Wrap("c".getBytes()), "3".getBytes());
//            segmentTwo.put(Bytes.Wrap("d".getBytes()), "4".getBytes());
//        }


//        public void CloseSegments()
//        {
//            if (iterator != null)
//            {
//                iterator.close();
//                iterator = null;
//            }
//            segmentOne.close();
//            segmentTwo.close();
//        }

//        [Xunit.Fact]
//        public void ShouldIterateOverAllSegments()
//        {
//            iterator = new SegmentIterator<>(
//                Array.asList(segmentOne, segmentTwo).iterator(),
//                hasNextCondition,
//                Bytes.Wrap("a".getBytes()),
//                Bytes.Wrap("z".getBytes()));

//            Assert.True(iterator.hasNext());
//            Assert.Equal("a", new string(iterator.peekNextKey().Get()));
//            Assert.Equal(KeyValuePair.Create("a", "1"), toStringKeyValue(iterator.MoveNext()));

//            Assert.True(iterator.hasNext());
//            Assert.Equal("b", new string(iterator.peekNextKey().Get()));
//            Assert.Equal(KeyValuePair.Create("b", "2"), toStringKeyValue(iterator.MoveNext()));

//            Assert.True(iterator.hasNext());
//            Assert.Equal("c", new string(iterator.peekNextKey().Get()));
//            Assert.Equal(KeyValuePair.Create("c", "3"), toStringKeyValue(iterator.MoveNext()));

//            Assert.True(iterator.hasNext());
//            Assert.Equal("d", new string(iterator.peekNextKey().Get()));
//            Assert.Equal(KeyValuePair.Create("d", "4"), toStringKeyValue(iterator.MoveNext()));

//            Assert.False(iterator.hasNext());
//        }

//        [Xunit.Fact]
//        public void ShouldNotThrowExceptionOnHasNextWhenStoreClosed()
//        {
//            iterator = new SegmentIterator<>(
//                Collections.singletonList(segmentOne).iterator(),
//                hasNextCondition,
//                Bytes.Wrap("a".getBytes()),
//                Bytes.Wrap("z".getBytes()));

//            iterator.currentIterator = segmentOne.all();
//            segmentOne.close();
//            Assert.False(iterator.hasNext());
//        }

//        [Xunit.Fact]
//        public void ShouldOnlyIterateOverSegmentsInRange()
//        {
//            iterator = new SegmentIterator<>(
//                Array.asList(segmentOne, segmentTwo).iterator(),
//                hasNextCondition,
//                Bytes.Wrap("a".getBytes()),
//                Bytes.Wrap("b".getBytes()));

//            Assert.True(iterator.hasNext());
//            Assert.Equal("a", new string(iterator.peekNextKey().Get()));
//            Assert.Equal(KeyValuePair.Create("a", "1"), toStringKeyValue(iterator.MoveNext()));

//            Assert.True(iterator.hasNext());
//            Assert.Equal("b", new string(iterator.peekNextKey().Get()));
//            Assert.Equal(KeyValuePair.Create("b", "2"), toStringKeyValue(iterator.MoveNext()));

//            Assert.False(iterator.hasNext());
//        }

//        [Xunit.Fact]// (expected = NoSuchElementException)
//        public void ShouldThrowNoSuchElementOnPeekNextKeyIfNoNext()
//        {
//            iterator = new SegmentIterator<>(
//                Array.asList(segmentOne, segmentTwo).iterator(),
//                hasNextCondition,
//                Bytes.Wrap("f".getBytes()),
//                Bytes.Wrap("h".getBytes()));

//            iterator.peekNextKey();
//        }

//        [Xunit.Fact]// (expected = NoSuchElementException)
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
























