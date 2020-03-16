using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors
{
    /**
     * Represents the state of a single thread running within a {@link KafkaStreams} application.
     */
    public class ThreadMetadata
    {
        public string ThreadName { get; }
        public string ThreadState { get; }
        public HashSet<TaskMetadata> ActiveTasks { get; }
        public HashSet<TaskMetadata> StandbyTasks { get; }
        public string MainConsumerClientId { get; }
        public string RestoreConsumerClientId { get; }
        public HashSet<string> ProducerClientIds { get; }

        // the admin client should be shared among all threads, so the client id should be the same;
        // we keep it at the thread-level for user's convenience and possible extensions in the future
        public string AdminClientId { get; }

        public ThreadMetadata(
            string threadName,
            string threadState,
            string mainConsumerClientId,
            string restoreConsumerClientId,
            HashSet<string> producerClientIds,
            string adminClientId,
            HashSet<TaskMetadata> activeTasks,
            HashSet<TaskMetadata> standbyTasks)
        {
            this.MainConsumerClientId = mainConsumerClientId;
            this.RestoreConsumerClientId = restoreConsumerClientId;
            this.ProducerClientIds = producerClientIds;
            this.AdminClientId = adminClientId;
            this.ThreadName = threadName;
            this.ThreadState = threadState;
            this.ActiveTasks = activeTasks;
            this.StandbyTasks = standbyTasks;
        }

        public string ConsumerClientId => MainConsumerClientId;

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            var that = (ThreadMetadata)o;
            return ThreadName.Equals(that.ThreadName)
                && ThreadState.Equals(that.ThreadState)
                && ActiveTasks.Equals(that.ActiveTasks)
                && StandbyTasks.Equals(that.StandbyTasks)
                && MainConsumerClientId.Equals(that.MainConsumerClientId)
                && RestoreConsumerClientId.Equals(that.RestoreConsumerClientId)
                && ProducerClientIds.Equals(that.ProducerClientIds)
                && AdminClientId.Equals(that.AdminClientId);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                ThreadName,
                ThreadState,
                ActiveTasks,
                StandbyTasks,
                MainConsumerClientId,
                RestoreConsumerClientId,
                ProducerClientIds,
                AdminClientId);
        }

        public override string ToString()
        {
            return "ThreadMetadata{" +
                    $"threadName={ThreadName}" +
                    $", threadState={ThreadState}" +
                    $", activeTasks={ActiveTasks }" +
                    $", standbyTasks={StandbyTasks}" +
                    $", consumerClientId={MainConsumerClientId}" +
                    $", restoreConsumerClientId={RestoreConsumerClientId}" +
                    $", producerClientIds={ProducerClientIds}" +
                    $", adminClientId={AdminClientId}" +
                    '}';
        }
    }
}