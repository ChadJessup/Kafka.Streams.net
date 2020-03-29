/*






 *

 *





 */










public class WallclockTimestampExtractorTest {

    [Xunit.Fact]
    public void extractSystemTimestamp() {
        TimestampExtractor extractor = new WallclockTimestampExtractor();

        long before = System.currentTimeMillis();
        long timestamp = extractor.extract(new ConsumeResult<>("anyTopic", 0, 0, null, null), 42);
        long after = System.currentTimeMillis();

        Assert.Equal(timestamp, is(new InBetween(before, after)));
    }

    private static class InBetween : BaseMatcher<long> {
        private long before;
        private long after;

        public InBetween(long before, long after) {
            this.before = before;
            this.after = after;
        }

        
        public bool matches(object item) {
            long timestamp = (long) item;
            return before <= timestamp && timestamp <= after;
        }

        
        public void describeMismatch(object item, Description mismatchDescription) {}

        
        public void describeTo(Description description) {}
    }

}
