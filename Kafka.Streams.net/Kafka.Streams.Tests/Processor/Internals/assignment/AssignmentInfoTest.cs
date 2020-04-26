using Confluent.Kafka;
using Kafka.Streams.State;
using Kafka.Streams.Tasks;
using Kafka.Streams.Temporary;
using System.Collections.Generic;
using Xunit;

namespace Kafka.Streams.Tests.Processor.Internals.Assignment
{
    public class AssignmentInfoTest
    {
        private List<TaskId> ActiveTasks = Arrays.asList(
            new TaskId(0, 0),
            new TaskId(0, 0),
            new TaskId(0, 1),
            new TaskId(1, 0));

        private Dictionary<TaskId, HashSet<TopicPartition>> StandbyTasks = new Dictionary<TaskId, HashSet<TopicPartition>>
        {
            { new TaskId(1, 1), new HashSet<TopicPartition>{ new TopicPartition("t1", 1), new TopicPartition("t2", 1) } },
            { new TaskId(2, 0), new HashSet<TopicPartition>{ new TopicPartition("t3", 0), new TopicPartition("t3", 0) } },
        };

        //private Dictionary<HostInfo, HashSet<TopicPartition>> globalAssignment = new HashMap<HostInfo, HashSet<TopicPartition>>();
        //{
        //        {
        //            Put(new HostInfo("localhost", 80),
        //                Utils.mkSet(new TopicPartition("t1", 1), new TopicPartition("t3", 3)));
        //        }
        //    };

        [Fact]
        public void ShouldUseLatestSupportedVersionByDefault()
        {
            AssignmentInfo info = new AssignmentInfo(ActiveTasks, StandbyTasks, globalAssignment);
            Assert.Equal(AssignmentInfo.LATEST_SUPPORTED_VERSION, info.version());
        }

        [Fact]// (expected = ArgumentException)
        public void ShouldThrowForUnknownVersion1()
        {
            new AssignmentInfo(0, ActiveTasks, StandbyTasks, globalAssignment, 0);
        }

        [Fact]// (expected = ArgumentException)
        public void ShouldThrowForUnknownVersion2()
        {
            new AssignmentInfo(AssignmentInfo.LATEST_SUPPORTED_VERSION + 1, ActiveTasks, StandbyTasks, globalAssignment, 0);
        }

        [Fact]
        public void ShouldEncodeAndDecodeVersion1()
        {
            AssignmentInfo info = new AssignmentInfo(1, ActiveTasks, StandbyTasks, globalAssignment, 0);
            AssignmentInfo expectedInfo = new AssignmentInfo(1, AssignmentInfo.UNKNOWN, ActiveTasks, StandbyTasks, Collections.emptyMap<HostInfo, HashSet<TopicPartition>>(), 0);
            Assert.Equal(expectedInfo, AssignmentInfo.decode(info.encode()));
        }

        [Fact]
        public void ShouldEncodeAndDecodeVersion2()
        {
            AssignmentInfo info = new AssignmentInfo(2, ActiveTasks, StandbyTasks, globalAssignment, 0);
            AssignmentInfo expectedInfo = new AssignmentInfo(2, AssignmentInfo.UNKNOWN, ActiveTasks, StandbyTasks, globalAssignment, 0);
            Assert.Equal(expectedInfo, AssignmentInfo.decode(info.encode()));
        }

        [Fact]
        public void ShouldEncodeAndDecodeVersion3()
        {
            AssignmentInfo info = new AssignmentInfo(3, ActiveTasks, StandbyTasks, globalAssignment, 0);
            AssignmentInfo expectedInfo = new AssignmentInfo(3, AssignmentInfo.LATEST_SUPPORTED_VERSION, ActiveTasks, StandbyTasks, globalAssignment, 0);
            Assert.Equal(expectedInfo, AssignmentInfo.decode(info.encode()));
        }

        [Fact]
        public void ShouldEncodeAndDecodeVersion4()
        {
            AssignmentInfo info = new AssignmentInfo(4, ActiveTasks, StandbyTasks, globalAssignment, 2);
            AssignmentInfo expectedInfo = new AssignmentInfo(4, AssignmentInfo.LATEST_SUPPORTED_VERSION, ActiveTasks, StandbyTasks, globalAssignment, 2);
            Assert.Equal(expectedInfo, AssignmentInfo.decode(info.encode()));
        }
    }
}
