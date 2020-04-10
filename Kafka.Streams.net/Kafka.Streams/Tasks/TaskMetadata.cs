
using Confluent.Kafka;
using System.Collections.Generic;

namespace Kafka.Streams.Processors
{
    /**
     * Represents the state of a single task running within a {@link KafkaStreams} application.
     */
    public class TaskMetadata
    {
        private readonly string taskId;

        private readonly HashSet<TopicPartition> topicPartitions;

        public TaskMetadata(
            string taskId,
            HashSet<TopicPartition> topicPartitions)
        {
            this.taskId = taskId;
            this.topicPartitions = topicPartitions;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var that = (TaskMetadata)o;
            return this.taskId.Equals(that.taskId)
                && this.topicPartitions.Equals(that.topicPartitions);
        }

        public override int GetHashCode()
        {
            return (this.taskId, this.topicPartitions).GetHashCode();
        }

        public override string ToString()
        {
            return "TaskMetadata{" +
                    "taskId=" + this.taskId +
                    ", topicPartitions=" + this.topicPartitions +
                    '}';
        }
    }
}
