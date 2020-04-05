using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.Tests.Helpers;
using Moq;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace Kafka.Streams.Tests.State.Internals
{
    public class TimestampedSegmentTest
    {
        [Xunit.Fact]
        public void ShouldDeleteStateDirectoryOnDestroy()
        {// throws Exception
            TimestampedSegment segment = new TimestampedSegment("segment", "window", 0L);
            string directoryPath = TestUtils.GetTempDirectory().FullName;
            DirectoryInfo directory = new DirectoryInfo(directoryPath);

            IProcessorContext mockContext = Mock.Of<IProcessorContext>();
            //expect(mockContext.AppConfigs()).andReturn(emptyMap());
            //expect(mockContext.stateDir).andReturn(directory);
            //replay(mockContext);

            segment.OpenDB(mockContext);

            Assert.True(new FileInfo(Path.Combine(directoryPath, "window")).Exists);
            Assert.True(new FileInfo(Path.Combine(directoryPath, "window", "segment")).Exists);
            //Assert.True(new FileInfo(Path.Combine(directoryPath, "window", "segment")).list().Length > 0);
            segment.Destroy();
            Assert.False(new FileInfo(Path.Combine(directoryPath, "window", "segment")).Exists);
            Assert.True(new FileInfo(Path.Combine(directoryPath, "window")).Exists);
        }

        [Xunit.Fact]
        public void ShouldBeEqualIfIdIsEqual()
        {
            TimestampedSegment segment = new TimestampedSegment("anyName", "anyName", 0L);
            TimestampedSegment segmentSameId = new TimestampedSegment("someOtherName", "someOtherName", 0L);
            TimestampedSegment segmentDifferentId = new TimestampedSegment("anyName", "anyName", 1L);

            Assert.Equal(segment, segment);
            Assert.Equal(segment, segmentSameId);
            Assert.NotEqual(segment, segmentDifferentId);
            Assert.Null(segment);
            //Assert.Equal(segment, "anyName");
        }

        [Xunit.Fact]
        public void ShouldHashOnSegmentIdOnly()
        {
            TimestampedSegment segment = new TimestampedSegment("anyName", "anyName", 0L);
            TimestampedSegment segmentSameId = new TimestampedSegment("someOtherName", "someOtherName", 0L);
            TimestampedSegment segmentDifferentId = new TimestampedSegment("anyName", "anyName", 1L);

            HashSet<TimestampedSegment> set = new HashSet<TimestampedSegment>();
            Assert.True(set.Add(segment));
            Assert.False(set.Add(segmentSameId));
            Assert.True(set.Add(segmentDifferentId));
        }

        [Xunit.Fact]
        public void ShouldCompareSegmentIdOnly()
        {
            TimestampedSegment segment1 = new TimestampedSegment("a", "C", 50L);
            TimestampedSegment segment2 = new TimestampedSegment("b", "B", 100L);
            TimestampedSegment segment3 = new TimestampedSegment("c", "A", 0L);

            Assert.Equal(0, segment1.CompareTo(segment1));
            Assert.Equal(-1, segment1.CompareTo(segment2));
            Assert.Equal(1, segment2.CompareTo(segment1));
            Assert.Equal(1, segment1.CompareTo(segment3));
            Assert.Equal(-1, segment3.CompareTo(segment1));
            Assert.Equal(1, segment2.CompareTo(segment3));
            Assert.Equal(-1, segment3.CompareTo(segment2));
        }
    }
}
