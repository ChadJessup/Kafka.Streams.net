//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */















//    public class OffsetCheckpointTest
//    {

//        private readonly string topic = "topic";

//        [Fact]
//        public void TestReadWrite()
//        { //throws IOException
//            File f = TestUtils.tempFile();
//            OffsetCheckpoint checkpoint = new OffsetCheckpoint(f);

//            try
//            {
//                Dictionary<TopicPartition, long> offsets = new HashMap<>();
//                offsets.Put(new TopicPartition(topic, 0), 0L);
//                offsets.Put(new TopicPartition(topic, 1), 1L);
//                offsets.Put(new TopicPartition(topic, 2), 2L);

//                checkpoint.write(offsets);
//                Assert.Equal(offsets, checkpoint.read());

//                checkpoint.Delete();
//                Assert.False(f.Exists);

//                offsets.Put(new TopicPartition(topic, 3), 3L);
//                checkpoint.write(offsets);
//                Assert.Equal(offsets, checkpoint.read());
//            }
//            finally
//            {
//                checkpoint.Delete();
//            }
//        }

//        [Fact]
//        public void ShouldNotWriteCheckpointWhenNoOffsets()
//        { //throws IOException
//          // we do not need to worry about file Name uniqueness since this file should not be created
//            File f = new FileInfo(TestUtils.GetTempDirectory().FullName, "kafka.tmp");
//            OffsetCheckpoint checkpoint = new OffsetCheckpoint(f);

//            checkpoint.write(Collections.< TopicPartition, long > emptyMap());

//            Assert.False(f.Exists);

//            Assert.Equal(Collections.< TopicPartition, long > emptyMap(), checkpoint.read());

//            // deleting a non-exist checkpoint file should be fine
//            checkpoint.Delete();
//        }
//    }
//}
///*






//*

//*





//*/















