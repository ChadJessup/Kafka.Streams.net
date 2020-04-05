//using System;
//using System.Collections.Generic;
//using System.IO;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */



























//    public class KeyValueSegmentsTest
//    {

//        private const int NUM_SEGMENTS = 5;
//        private const long SEGMENT_INTERVAL = 100L;
//        private const long RETENTION_PERIOD = 4 * SEGMENT_INTERVAL;
//        private InternalMockProcessorContext context;
//        private KeyValueSegments segments;
//        private DirectoryInfo stateDirectory;
//        private readonly string storeName = "test";


//        public void CreateContext()
//        {
//            stateDirectory = TestUtils.GetTempDirectory();
//            context = new InternalMockProcessorContext(
//                stateDirectory,
//                Serdes.String(),
//                Serdes.Long(),
//                new NoOpRecordCollector(),
//                new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
//            );
//            segments = new KeyValueSegments(storeName, RETENTION_PERIOD, SEGMENT_INTERVAL);
//        }


//        public void Close()
//        {
//            segments.close();
//        }

//        [Fact]
//        public void ShouldGetSegmentIdsFromTimestamp()
//        {
//            Assert.Equal(0, segments.segmentId(0));
//            Assert.Equal(1, segments.segmentId(SEGMENT_INTERVAL));
//            Assert.Equal(2, segments.segmentId(2 * SEGMENT_INTERVAL));
//            Assert.Equal(3, segments.segmentId(3 * SEGMENT_INTERVAL));
//        }

//        [Fact]
//        public void ShouldBaseSegmentIntervalOnRetentionAndNumSegments()
//        {
//            KeyValueSegments segments = new KeyValueSegments("test", 8 * SEGMENT_INTERVAL, 2 * SEGMENT_INTERVAL);
//            Assert.Equal(0, segments.segmentId(0));
//            Assert.Equal(0, segments.segmentId(SEGMENT_INTERVAL));
//            Assert.Equal(1, segments.segmentId(2 * SEGMENT_INTERVAL));
//        }

//        [Fact]
//        public void ShouldGetSegmentNameFromId()
//        {
//            Assert.Equal("test.0", segments.segmentName(0));
//            Assert.Equal("test." + SEGMENT_INTERVAL, segments.segmentName(1));
//            Assert.Equal("test." + 2 * SEGMENT_INTERVAL, segments.segmentName(2));
//        }

//        [Fact]
//        public void ShouldCreateSegments()
//        {
//            KeyValueSegment segment1 = segments.getOrCreateSegmentIfLive(0, context, -1L);
//            KeyValueSegment segment2 = segments.getOrCreateSegmentIfLive(1, context, -1L);
//            KeyValueSegment segment3 = segments.getOrCreateSegmentIfLive(2, context, -1L);
//            Assert.True(new DirectoryInfo(Path.Combine(context.stateDir(), "test/test.0")).isDirectory());
//            Assert.True(new DirectoryInfo(Path.Combine(context.stateDir(), "test/test." + SEGMENT_INTERVAL)).isDirectory());
//            Assert.True(new DirectoryInfo(Path.Combine(context.stateDir(), "test/test." + 2 * SEGMENT_INTERVAL)).isDirectory());
//            Assert.True(segment1.isOpen());
//            Assert.True(segment2.isOpen());
//            Assert.True(segment3.isOpen());
//        }

//        [Fact]
//        public void ShouldNotCreateSegmentThatIsAlreadyExpired()
//        {
//            long streamTime = UpdateStreamTimeAndCreateSegment(7);
//            Assert.Null(segments.getOrCreateSegmentIfLive(0, context, streamTime));
//            Assert.False(new DirectoryInfo(Path.Combine(context.stateDir(), "test/test.0")).Exists);
//        }

//        [Fact]
//        public void ShouldCleanupSegmentsThatHaveExpired()
//        {
//            KeyValueSegment segment1 = segments.getOrCreateSegmentIfLive(0, context, -1L);
//            KeyValueSegment segment2 = segments.getOrCreateSegmentIfLive(1, context, -1L);
//            KeyValueSegment segment3 = segments.getOrCreateSegmentIfLive(7, context, SEGMENT_INTERVAL * 7L);
//            Assert.False(segment1.isOpen());
//            Assert.False(segment2.isOpen());
//            Assert.True(segment3.isOpen());
//            Assert.False(new DirectoryInfo(Path.Combine(context.stateDir(), "test/test.0")).Exists);
//            Assert.False(new DirectoryInfo(Path.Combine(context.stateDir(), "test/test." + SEGMENT_INTERVAL)).Exists);
//            Assert.True( new DirectoryInfo(Path.Combine(context.stateDir(), "test/test." + 7 * SEGMENT_INTERVAL)).Exists);
//        }

//        [Fact]
//        public void ShouldGetSegmentForTimestamp()
//        {
//            KeyValueSegment segment = segments.getOrCreateSegmentIfLive(0, context, -1L);
//            segments.getOrCreateSegmentIfLive(1, context, -1L);
//            Assert.Equal(segment, segments.getSegmentForTimestamp(0L));
//        }

//        [Fact]
//        public void ShouldGetCorrectSegmentString()
//        {
//            KeyValueSegment segment = segments.getOrCreateSegmentIfLive(0, context, -1L);
//            Assert.Equal("KeyValueSegment(id=0, name=test.0)", segment.ToString());
//        }

//        [Fact]
//        public void ShouldCloseAllOpenSegments()
//        {
//            KeyValueSegment first = segments.getOrCreateSegmentIfLive(0, context, -1L);
//            KeyValueSegment second = segments.getOrCreateSegmentIfLive(1, context, -1L);
//            KeyValueSegment third = segments.getOrCreateSegmentIfLive(2, context, -1L);
//            segments.close();

//            Assert.False(first.isOpen());
//            Assert.False(second.isOpen());
//            Assert.False(third.isOpen());
//        }

//        [Fact]
//        public void ShouldOpenExistingSegments()
//        {
//            segments = new KeyValueSegments("test", 4, 1);
//            segments.getOrCreateSegmentIfLive(0, context, -1L);
//            segments.getOrCreateSegmentIfLive(1, context, -1L);
//            segments.getOrCreateSegmentIfLive(2, context, -1L);
//            segments.getOrCreateSegmentIfLive(3, context, -1L);
//            segments.getOrCreateSegmentIfLive(4, context, -1L);
//            // close existing.
//            segments.close();

//            segments = new KeyValueSegments("test", 4, 1);
//            segments.openExisting(context, -1L);

//            Assert.True(segments.getSegmentForTimestamp(0).isOpen());
//            Assert.True(segments.getSegmentForTimestamp(1).isOpen());
//            Assert.True(segments.getSegmentForTimestamp(2).isOpen());
//            Assert.True(segments.getSegmentForTimestamp(3).isOpen());
//            Assert.True(segments.getSegmentForTimestamp(4).isOpen());
//        }

//        [Fact]
//        public void ShouldGetSegmentsWithinTimeRange()
//        {
//            UpdateStreamTimeAndCreateSegment(0);
//            UpdateStreamTimeAndCreateSegment(1);
//            UpdateStreamTimeAndCreateSegment(2);
//            UpdateStreamTimeAndCreateSegment(3);
//            long streamTime = UpdateStreamTimeAndCreateSegment(4);
//            segments.getOrCreateSegmentIfLive(0, context, streamTime);
//            segments.getOrCreateSegmentIfLive(1, context, streamTime);
//            segments.getOrCreateSegmentIfLive(2, context, streamTime);
//            segments.getOrCreateSegmentIfLive(3, context, streamTime);
//            segments.getOrCreateSegmentIfLive(4, context, streamTime);

//            List<KeyValueSegment> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL);
//            Assert.Equal(3, segments.Count);
//            Assert.Equal(0, segments[0].id);
//            Assert.Equal(1, segments[1].id);
//            Assert.Equal(2, segments[2].id);
//        }

//        [Fact]
//        public void ShouldGetSegmentsWithinTimeRangeOutOfOrder()
//        {
//            UpdateStreamTimeAndCreateSegment(4);
//            UpdateStreamTimeAndCreateSegment(2);
//            UpdateStreamTimeAndCreateSegment(0);
//            UpdateStreamTimeAndCreateSegment(1);
//            UpdateStreamTimeAndCreateSegment(3);

//            List<KeyValueSegment> segments = this.segments.segments(0, 2 * SEGMENT_INTERVAL);
//            Assert.Equal(3, segments.Count);
//            Assert.Equal(0, segments[0].id);
//            Assert.Equal(1, segments[1].id);
//            Assert.Equal(2, segments[2].id);
//        }

//        [Fact]
//        public void ShouldRollSegments()
//        {
//            UpdateStreamTimeAndCreateSegment(0);
//            VerifyCorrectSegments(0, 1);
//            UpdateStreamTimeAndCreateSegment(1);
//            VerifyCorrectSegments(0, 2);
//            UpdateStreamTimeAndCreateSegment(2);
//            VerifyCorrectSegments(0, 3);
//            UpdateStreamTimeAndCreateSegment(3);
//            VerifyCorrectSegments(0, 4);
//            UpdateStreamTimeAndCreateSegment(4);
//            VerifyCorrectSegments(0, 5);
//            UpdateStreamTimeAndCreateSegment(5);
//            VerifyCorrectSegments(1, 5);
//            UpdateStreamTimeAndCreateSegment(6);
//            VerifyCorrectSegments(2, 5);
//        }

//        [Fact]
//        public void FutureEventsShouldNotCauseSegmentRoll()
//        {
//            UpdateStreamTimeAndCreateSegment(0);
//            VerifyCorrectSegments(0, 1);
//            UpdateStreamTimeAndCreateSegment(1);
//            VerifyCorrectSegments(0, 2);
//            UpdateStreamTimeAndCreateSegment(2);
//            VerifyCorrectSegments(0, 3);
//            UpdateStreamTimeAndCreateSegment(3);
//            VerifyCorrectSegments(0, 4);
//            long streamTime = UpdateStreamTimeAndCreateSegment(4);
//            VerifyCorrectSegments(0, 5);
//            segments.getOrCreateSegmentIfLive(5, context, streamTime);
//            VerifyCorrectSegments(0, 6);
//            segments.getOrCreateSegmentIfLive(6, context, streamTime);
//            VerifyCorrectSegments(0, 7);
//        }

//        private long UpdateStreamTimeAndCreateSegment(int segment)
//        {
//            long streamTime = SEGMENT_INTERVAL * segment;
//            segments.getOrCreateSegmentIfLive(segment, context, streamTime);
//            return streamTime;
//        }

//        [Fact]
//        public void ShouldUpdateSegmentFileNameFromOldDateFormatToNewFormat()
//        {// throws Exception
//            long segmentInterval = 60_000L; // the old segment file's naming system maxes out at 1 minute granularity.

//            segments = new KeyValueSegments(storeName, NUM_SEGMENTS * segmentInterval, segmentInterval);

//            string storeDirectoryPath = Path.Combine(stateDirectory.FullName, storeName);
//            DirectoryInfo storeDirectory = new DirectoryInfo(storeDirectoryPath);
//            //noinspection ResultOfMethodCallIgnored
//            storeDirectory.mkdirs();

//            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
//            formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));

//            for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId)
//            {
//                DirectoryInfo oldSegment = new DirectoryInfo(Path.Combine(storeDirectoryPath, storeName + "-" + formatter.format(new DateTime(segmentId * segmentInterval))));
//                //noinspection ResultOfMethodCallIgnored
//                //oldSegment.createNewFile();
//            }

//            segments.openExisting(context, -1L);

//            for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId)
//            {
//                string segmentName = storeName + "." + (long)segmentId * segmentInterval;
//                DirectoryInfo newSegment = new DirectoryInfo(storeDirectoryPath + Path.DirectorySeparatorChar + segmentName);
//                Assert.True(newSegment.Exists);
//            }
//        }

//        [Fact]
//        public void ShouldUpdateSegmentFileNameFromOldColonFormatToNewFormat()
//        {// throws Exception
//            string storeDirectoryPath = stateDirectory.FullName + Path.DirectorySeparatorChar + storeName;
//            DirectoryInfo storeDirectory = new DirectoryInfo(storeDirectoryPath);
//            //noinspection ResultOfMethodCallIgnored
//            storeDirectory.mkdirs();

//            for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId)
//            {
//                DirectoryInfo oldSegment = new DirectoryInfo(storeDirectoryPath + Path.DirectorySeparatorChar + storeName + ":" + segmentId * (RETENTION_PERIOD / (NUM_SEGMENTS - 1)));
//                //noinspection ResultOfMethodCallIgnored
//                //oldSegment.createNewFile();
//            }

//            segments.openExisting(context, -1L);

//            for (int segmentId = 0; segmentId < NUM_SEGMENTS; ++segmentId)
//            {
//                DirectoryInfo newSegment = new DirectoryInfo(storeDirectoryPath + Path.DirectorySeparatorChar + storeName + "." + segmentId * (RETENTION_PERIOD / (NUM_SEGMENTS - 1)));
//                Assert.True(newSegment.Exists);
//            }
//        }

//        [Fact]
//        public void ShouldClearSegmentsOnClose()
//        {
//            segments.getOrCreateSegmentIfLive(0, context, -1L);
//            segments.close();
//            Assert.Equal(segments.getSegmentForTimestamp(0), nullValue());
//        }

//        private void VerifyCorrectSegments(long first, int numSegments)
//        {
//            List<KeyValueSegment> result = this.segments.segments(0, long.MaxValue);
//            Assert.Equal(numSegments, result.Count);
//            for (int i = 0; i < numSegments; i++)
//            {
//                Assert.Equal(i + first, result[i].id);
//            }
//        }
//    }
//}
///*






//*

//*





//*/



























