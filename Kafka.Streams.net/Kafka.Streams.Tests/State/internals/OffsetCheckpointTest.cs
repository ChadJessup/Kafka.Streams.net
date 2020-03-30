/*






 *

 *





 */















public class OffsetCheckpointTest {

    private readonly string topic = "topic";

    [Xunit.Fact]
    public void TestReadWrite(){ //throws IOException
        File f = TestUtils.tempFile();
        OffsetCheckpoint checkpoint = new OffsetCheckpoint(f);

        try {
            Dictionary<TopicPartition, long> offsets = new HashMap<>();
            offsets.put(new TopicPartition(topic, 0), 0L);
            offsets.put(new TopicPartition(topic, 1), 1L);
            offsets.put(new TopicPartition(topic, 2), 2L);

            checkpoint.write(offsets);
            Assert.Equal(offsets, checkpoint.read());

            checkpoint.delete();
            Assert.False(f.exists());

            offsets.put(new TopicPartition(topic, 3), 3L);
            checkpoint.write(offsets);
            Assert.Equal(offsets, checkpoint.read());
        } finally {
            checkpoint.delete();
        }
    }

    [Xunit.Fact]
    public void ShouldNotWriteCheckpointWhenNoOffsets(){ //throws IOException
        // we do not need to worry about file name uniqueness since this file should not be created
        File f = new File(TestUtils.tempDirectory().getAbsolutePath(), "kafka.tmp");
        OffsetCheckpoint checkpoint = new OffsetCheckpoint(f);

        checkpoint.write(Collections.<TopicPartition, long>emptyMap());

        Assert.False(f.exists());

        Assert.Equal(Collections.<TopicPartition, long>emptyMap(), checkpoint.read());

        // deleting a non-exist checkpoint file should be fine
        checkpoint.delete();
    }
}
