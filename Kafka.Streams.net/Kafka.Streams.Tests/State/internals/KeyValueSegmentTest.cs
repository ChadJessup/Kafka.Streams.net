//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.Tests.Helpers;
//using System.Collections.Generic;
//using System.IO;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class KeyValueSegmentTest
//    {

//        [Fact]
//        public void ShouldDeleteStateDirectoryOnDestroy()
//        {// throws Exception
//            KeyValueSegment segment = new KeyValueSegment("segment", "window", 0L);
//            string directoryPath = TestUtils.GetTempDirectory().FullName;
//            var directory = new DirectoryInfo(directoryPath);

//            IProcessorContext mockContext = mock(ProcessorContext);
//            expect(mockContext.AppConfigs).andReturn(emptyMap());
//            expect(mockContext.stateDir).andReturn(directory);
//            replay(mockContext);

//            segment.OpenDB(mockContext);

//            Assert.True( new DirectoryInfo(Path.Combine(directoryPath, "window")).Exists);
//            Assert.True( new DirectoryInfo(Path.Combine(directoryPath ,"window", "segment")).Exists);
//            //Assert.True( new DirectoryInfo(Path.Combine(directoryPath,"window", "segment")).list().Length > 0);
//            segment.Destroy();
//            Assert.False(new DirectoryInfo(Path.Combine(directoryPath,"window", "segment")).Exists);
//            Assert.True( new DirectoryInfo(Path.Combine(directoryPath, "window")).Exists);
//        }

//        [Fact]
//        public void ShouldBeEqualIfIdIsEqual()
//        {
//            KeyValueSegment segment = new KeyValueSegment("anyName", "anyName", 0L);
//            KeyValueSegment segmentSameId = new KeyValueSegment("someOtherName", "someOtherName", 0L);
//            KeyValueSegment segmentDifferentId = new KeyValueSegment("anyName", "anyName", 1L);

//            Assert.Equal(segment, segment);
//            Assert.Equal(segment, segmentSameId);
//            Assert.NotEqual(segment, segmentDifferentId);
//            Assert.NotNull(segment);
//            // Assert.NotEqual(segment, "anyName");
//        }

//        [Fact]
//        public void ShouldHashOnSegmentIdOnly()
//        {
//            KeyValueSegment segment = new KeyValueSegment("anyName", "anyName", 0L);
//            KeyValueSegment segmentSameId = new KeyValueSegment("someOtherName", "someOtherName", 0L);
//            KeyValueSegment segmentDifferentId = new KeyValueSegment("anyName", "anyName", 1L);

//            HashSet<KeyValueSegment> set = new HashSet<KeyValueSegment>();
//            Assert.True(set.Add(segment));
//            Assert.False(set.Add(segmentSameId));
//            Assert.True(set.Add(segmentDifferentId));
//        }

//        [Fact]
//        public void ShouldCompareSegmentIdOnly()
//        {
//            KeyValueSegment segment1 = new KeyValueSegment("a", "C", 50L);
//            KeyValueSegment segment2 = new KeyValueSegment("b", "B", 100L);
//            KeyValueSegment segment3 = new KeyValueSegment("c", "A", 0L);

//            Assert.Equal(0, segment1.CompareTo(segment1));
//            Assert.Equal(-1, segment1.CompareTo(segment2));
//            Assert.Equal(1, segment2.CompareTo(segment1));
//            Assert.Equal(1, segment1.CompareTo(segment3));
//            Assert.Equal(-1, segment3.CompareTo(segment1));
//            Assert.Equal(1, segment2.CompareTo(segment3));
//            Assert.Equal(-1, segment3.CompareTo(segment2));
//        }
//    }
//}
