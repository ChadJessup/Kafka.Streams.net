namespace Kafka.Streams.Tests.Processor.Internals.assignment
{
    /*






    *

    *





    */

















    public class AssignmentInfoTest
    {
        private List<TaskId> activeTasks = Arrays.asList(
            new TaskId(0, 0),
            new TaskId(0, 0),
            new TaskId(0, 1), new TaskId(1, 0));
        private Dictionary<TaskId, HashSet<TopicPartition>> standbyTasks = new HashMap<TaskId, HashSet<TopicPartition>>() {
        {
            Put(new TaskId(1, 1),
                Utils.mkSet(new TopicPartition("t1", 1), new TopicPartition("t2", 1)));
        Put(new TaskId(2, 0),
                Utils.mkSet(new TopicPartition("t3", 0), new TopicPartition("t3", 0)));
        }
};
private Dictionary<HostInfo, HashSet<TopicPartition>> globalAssignment = new HashMap<HostInfo, HashSet<TopicPartition>>() {
        {
            Put(new HostInfo("localhost", 80),
                Utils.mkSet(new TopicPartition("t1", 1), new TopicPartition("t3", 3)));
        }
    };

    [Fact]
public void ShouldUseLatestSupportedVersionByDefault()
{
    AssignmentInfo info = new AssignmentInfo(activeTasks, standbyTasks, globalAssignment);
    Assert.Equal(AssignmentInfo.LATEST_SUPPORTED_VERSION, info.version());
}

[Fact]// (expected = ArgumentException)
public void ShouldThrowForUnknownVersion1()
{
    new AssignmentInfo(0, activeTasks, standbyTasks, globalAssignment, 0);
}

[Fact]// (expected = ArgumentException)
public void ShouldThrowForUnknownVersion2()
{
    new AssignmentInfo(AssignmentInfo.LATEST_SUPPORTED_VERSION + 1, activeTasks, standbyTasks, globalAssignment, 0);
}

[Fact]
public void ShouldEncodeAndDecodeVersion1()
{
    AssignmentInfo info = new AssignmentInfo(1, activeTasks, standbyTasks, globalAssignment, 0);
    AssignmentInfo expectedInfo = new AssignmentInfo(1, AssignmentInfo.UNKNOWN, activeTasks, standbyTasks, Collections.< HostInfo, HashSet < TopicPartition >> emptyMap(), 0);
    Assert.Equal(expectedInfo, AssignmentInfo.decode(info.encode()));
}

[Fact]
public void ShouldEncodeAndDecodeVersion2()
{
    AssignmentInfo info = new AssignmentInfo(2, activeTasks, standbyTasks, globalAssignment, 0);
    AssignmentInfo expectedInfo = new AssignmentInfo(2, AssignmentInfo.UNKNOWN, activeTasks, standbyTasks, globalAssignment, 0);
    Assert.Equal(expectedInfo, AssignmentInfo.decode(info.encode()));
}

[Fact]
public void ShouldEncodeAndDecodeVersion3()
{
    AssignmentInfo info = new AssignmentInfo(3, activeTasks, standbyTasks, globalAssignment, 0);
    AssignmentInfo expectedInfo = new AssignmentInfo(3, AssignmentInfo.LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, globalAssignment, 0);
    Assert.Equal(expectedInfo, AssignmentInfo.decode(info.encode()));
}

[Fact]
public void ShouldEncodeAndDecodeVersion4()
{
    AssignmentInfo info = new AssignmentInfo(4, activeTasks, standbyTasks, globalAssignment, 2);
    AssignmentInfo expectedInfo = new AssignmentInfo(4, AssignmentInfo.LATEST_SUPPORTED_VERSION, activeTasks, standbyTasks, globalAssignment, 2);
    Assert.Equal(expectedInfo, AssignmentInfo.decode(info.encode()));
}
}
}
/*






*

*





*/
























