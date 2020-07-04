//using Kafka.Streams.Interfaces;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor
//{
//    public class WallclockTimestampExtractorTest
//    {
//        [Fact]
//        public void ExtractSystemTimestamp()
//        {
//            ITimestampExtractor extractor = new WallclockTimestampExtractor();

//            long before = System.currentTimeMillis();
//            long timestamp = extractor.extract(new ConsumeResult<>("anyTopic", 0, 0, null, null), 42);
//            long after = System.currentTimeMillis();

//            Assert.Equal(timestamp, new InBetween(before, after));
//        }

//        private class InBetween : BaseMatcher<long>
//        {
//            private readonly long before;
//            private readonly long after;

//            public InBetween(long before, long after)
//            {
//                this.before = before;
//                this.after = after;
//            }

//            public bool Matches(object item)
//            {
//                long timestamp = (long)item;
//                return before <= timestamp && timestamp <= after;
//            }


//            public void DescribeMismatch(object item, Description mismatchDescription)
//            {
//            }

//            public void DescribeTo(Description description)
//            {
//            }
//        }
//    }
//}
