/*






 *

 *





 */



























public class TimetampedSegmentsTest {

    private static int NUM_SEGMENTS = 5;
    private static long SEGMENT_INTERVAL = 100L;
    private static long RETENTION_PERIOD = 4 * SEGMENT_INTERVAL;
    private InternalMockProcessorContext context;
    private TimestampedSegments segments;
    private File stateDirectory;
    private string storeName = "test";

    
    public void createContext() {
        stateDirectory = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext(
            stateDirectory,
            Serdes.String(),
            Serdes.Long(),
            new NoOpRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
        segments = new TimestampedSegments(storeName, RETENTION_PERIOD, SEGMENT_INTERVAL);
    }

    
    public void close() {
        segments.close();
    }

    [Xunit.Fact]
    public void shouldGetSegmentIdsFromTimestamp() {
        Assert.Equal(0, segments.segmentId(0));
        Assert.Equal(1, segments.segmentId(SEGMENT_INTERVAL));
        Assert.Equal(2, segments.segmentId(2 * SEGMENT_INTERVAL));
        Assert.Equal(3, segments.segmentId(3 * SEGMENT_INTERVAL));
    }

    [Xunit.Fact]
    public void shouldBaseSegmentIntervalOnRetentionAndNumSegments() {
        KeyValueSegments segments = new KeyValueSegments("test", 8 * SEGMENT_INTERVAL, 2 * SEGMENT_INTERVAL);
        Assert.Equal(0, segments.segmentId(0));
        Assert.Equal(0, segments.segmentId(SEGMENT_INTERVAL));
        Assert.Equal(1, segments.segmentId(2 * SEGMENT_INTERVAL));
    }

    [Xunit.Fact]
    public void shouldGetSegmentNameFromId() {
        Assert.Equal("test.0", segments.segmentName(0));
        Assert.Equal("test." + SEGMENT_INTERVAL, segments.segmentName(1));
        Assert.Equal("test." + 2 * SEGMENT_INTERVAL, segments.segmentName(2));
    }

    [Xunit.Fact]
    public void shouldCreateSegments() {
        TimestampedSegment segment1 = segments.getOrCreateSegmentIfLive(0, context, -1L);
        TimestampedSegment segment2 = segments.getOrCreateSegmentIfLive(1, context, -1L);
        TimestampedSegment segment3 = segments.getOrCreateSegmentIfLive(2, context, -1L);
        Assert.True(new File(context.stateDir(), "test/test.0").isDirectory());
        Assert.True(new File(context.stateDir(), "test/test." + SEGMENT_INTERVAL).isDirectory());
        Assert.True(new File(context.stateDir(), "test/test." + 2 * SEGMENT_INTERVAL).isDirectory());
        Assert.True(segment1.isOpen());
        Assert.True(segment2.isOpen());
        Assert.True(segment3.isOpen());
    }

    [Xunit.Fact]
    public void shouldNotCreateSegmentThatIsAlreadyExpired() {
        long streamTime = updateStreamTimeAndCreateSegment(7);
        assertNull(segments.getOrCreateSegmentIfLive(0, context, streamTime));
        Assert.False(new File(context.stateDir(), "test/test.0").exists());
    }

    [Xunit.Fact]
    public void shouldCleanupSegmentsThatHaveExpired() {
        TimestampedSegment segment1 = segments.getOrCreateSegmentIfLive(0, context, -1L);
        TimestampedSegment segment2 = segments.getOrCreateSegmentIfLive(1, context, -1L);
        TimestampedSegment segment3 = segments.getOrCreateSegmentIfLive(7, context, SEGMENT_INTERVAL * 7L);
        Assert.False(segment1.isOpen());
        Assert.False(segment2.isOpen());
        Assert.True(segment3.isOpen());
        Assert.False(new File(context.stateDir(), "test/test.0").exists());
        Assert.False(new File(context.stateDir(), "test/test." + SEGMENT_INTERVAL).exists());
        Assert.True(new File(context.stateDir(), "test/test." + 7 * SEGMENT_INTERVAL).exists());
    }

    [Xunit.Fact]
    public void shouldGetSegmentForTimestamp() {
        TimestampedSegment segment = segments.getOrCreateSegmentIfLive(0, context, -1L);
        segments.getOrCreateSegmentIfLive(1, context, -1L);
        Assert.Equal(segment, segments.getSegmentForTimestamp(0L));
    }

    [Xunit.Fact]
    public void shouldGetCorrectSegmentString() {
        TimestampedSegment segment = segments.getOrCreateSegmentIfLive(0, context, -1L);
        Assert.Equal("TimestampedSegment(id=0, name=test.0)", segment.toString());
    }

    [Xunit.Fact]
    public void shouldCloseAllOpenSegments() {
        TimestampedSegment first = segments.getOrCreateSegmentIfLive(0, context, -1L);
        TimestampedSegment second = segments.getOrCreateSegmentIfLive(1, context, -1L);
        TimestampedSegment third = segments.getOrCreateSegmentIfLive(2, context, -1L);
        segments.close();

        Assert.False(first.isOpen());
        Assert.False(second.isOpen());
        Assert.False(third.isOpen());
    }

    [Xunit.Fact]
    public void shouldOpenExistingSegments() {
        segments = new TimestampedSegments("test", 4, 1);
        segments.getOrCreateSegmentIfLive(0, context, -1L);
        segments.getOrCreateSegmentIfLive(1, context, -1L);
        segments.getOrCreateSegmentIfLive(2, context, -1L);
        segments.getOrCreateSegmentIfLive(3, context, -1L);
        segments.getOrCreateSegmentIfLive(4, context, -1L);
        // close existing.
        segments.close();

        segments = new TimestampedSegments("test", 4, 1);
        segments.openExisting(context, -1L);

        Assert.True(segments.getSegmentForTimestamp(0).isOpen());
        Assert.True(segments.getSegmentForTimestamp(1).isOpen());
        Assert.True(segments.getSegmentForTimestamp(2).isOpen());
        Assert.True(segments.getSegmentForTimestamp(3).isOpen());
        Assert.True(segments.getSegmentForTimestamp(4).isOpen());
    }

    [Xunit.Fact]
    public void shouldGetSegmentsWithinTimeRange() {
        updateStreamTimeAndCreateSegment(0);
        updateStreamTimeAndCreateSegment(1);
        updateStreamTimeAndCreateSegment(2);
        updateStreamTimeAndCreateSegment(3);
        long streamTime = updateStreamTimeAndCreateSegment(4);
        segments.getOrCreateSegmentIfLive(0, context, streamTime);
        segments.getOrCreateSegmentIfLive(1, context, streamTime);
        segments.getOrCreateSegmentIfLive(2, context, streamTime);
        segments.getOrCreateSegmentIfLive(3, context, streamTime);
        segments.getOrCreateSegmentIfLive(4, context, streamTime);

        List<TimestampedSegment> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL);
        Assert.Equal(3, segments.Count);
        Assert.Equal(0, segments.get(0).id);
        Assert.Equal(1, segments.get(1).id);
        Assert.Equal(2, segments.get(2).id);
    }

    [Xunit.Fact]
    public void shouldGetSegmentsWithinTimeRangeOutOfOrder() {
        updateStreamTimeAndCreateSegment(4);
        updateStreamTimeAndCreateSegment(2);
        updateStreamTimeAndCreateSegment(0);
        updateStreamTimeAndCreateSegment(1);
        updateStreamTimeAndCreateSegment(3);

        List<TimestampedSegment> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL);
        Assert.Equal(3, segments.Count);
        Assert.Equal(0, segments.get(0).id);
        Assert.Equal(1, segments.get(1).id);
        Assert.Equal(2, segments.get(2).id);
    }

    [Xunit.Fact]
    public void shouldRollSegments() {
        updateStreamTimeAndCreateSegment(0);
        verifyCorrectSegments(0, 1);
        updateStreamTimeAndCreateSegment(1);
        verifyCorrectSegments(0, 2);
        updateStreamTimeAndCreateSegment(2);
        verifyCorrectSegments(0, 3);
        updateStreamTimeAndCreateSegment(3);
        verifyCorrectSegments(0, 4);
        updateStreamTimeAndCreateSegment(4);
        verifyCorrectSegments(0, 5);
        updateStreamTimeAndCreateSegment(5);
        verifyCorrectSegments(1, 5);
        updateStreamTimeAndCreateSegment(6);
        verifyCorrectSegments(2, 5);
    }

    [Xunit.Fact]
    public void futureEventsShouldNotCauseSegmentRoll() {
        updateStreamTimeAndCreateSegment(0);
        verifyCorrectSegments(0, 1);
        updateStreamTimeAndCreateSegment(1);
        verifyCorrectSegments(0, 2);
        updateStreamTimeAndCreateSegment(2);
        verifyCorrectSegments(0, 3);
        updateStreamTimeAndCreateSegment(3);
        verifyCorrectSegments(0, 4);
        long streamTime = updateStreamTimeAndCreateSegment(4);
        verifyCorrectSegments(0, 5);
        segments.getOrCreateSegmentIfLive(5, context, streamTime);
        verifyCorrectSegments(0, 6);
        segments.getOrCreateSegmentIfLive(6, context, streamTime);
        verifyCorrectSegments(0, 7);
    }

    private long updateStreamTimeAndCreateSegment(int segment) {
        long streamTime = SEGMENT_INTERVAL * segment;
        segments.getOrCreateSegmentIfLive(segment, context, streamTime);
        return streamTime;
    }

    [Xunit.Fact]
    public void shouldUpdateSegmentFileNameFromOldDateFormatToNewFormat() {// throws Exception
        long segmentInterval = 60_000L; // the old segment file's naming system maxes out at 1 minute granularity.

        segments = new TimestampedSegments(storeName, NUM_SEGMENTS * segmentInterval, segmentInterval);

        string storeDirectoryPath = stateDirectory.getAbsolutePath() + File.separator + storeName;
        File storeDirectory = new File(storeDirectoryPath);
        //noinspection ResultOfMethodCallIgnored
        storeDirectory.mkdirs();

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));

        for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId) {
            File oldSegment = new File(storeDirectoryPath + File.separator + storeName + "-" + formatter.format(new Date(segmentId * segmentInterval)));
            //noinspection ResultOfMethodCallIgnored
            oldSegment.createNewFile();
        }

        segments.openExisting(context, -1L);

        for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId) {
            string segmentName = storeName + "." + (long) segmentId * segmentInterval;
            File newSegment = new File(storeDirectoryPath + File.separator + segmentName);
            Assert.True(newSegment.exists());
        }
    }

    [Xunit.Fact]
    public void shouldUpdateSegmentFileNameFromOldColonFormatToNewFormat() {// throws Exception
        string storeDirectoryPath = stateDirectory.getAbsolutePath() + File.separator + storeName;
        File storeDirectory = new File(storeDirectoryPath);
        //noinspection ResultOfMethodCallIgnored
        storeDirectory.mkdirs();

        for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId) {
            File oldSegment = new File(storeDirectoryPath + File.separator + storeName + ":" + segmentId * (RETENTION_PERIOD / (NUM_SEGMENTS - 1)));
            //noinspection ResultOfMethodCallIgnored
            oldSegment.createNewFile();
        }

        segments.openExisting(context, -1L);

        for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId) {
            File newSegment = new File(storeDirectoryPath + File.separator + storeName + "." + segmentId * (RETENTION_PERIOD / (NUM_SEGMENTS - 1)));
            Assert.True(newSegment.exists());
        }
    }

    [Xunit.Fact]
    public void shouldClearSegmentsOnClose() {
        segments.getOrCreateSegmentIfLive(0, context, -1L);
        segments.close();
        Assert.Equal(segments.getSegmentForTimestamp(0), is(nullValue()));
    }

    private void verifyCorrectSegments(long first, int numSegments) {
        List<TimestampedSegment> result = this.segments.segments(0, long.MaxValue);
        Assert.Equal(numSegments, result.Count);
        for (int i = 0; i < numSegments; i++) {
            Assert.Equal(i + first, result.get(i).id);
        }
    }
}
