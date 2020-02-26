using System.Collections.Generic;

namespace Kafka.Streams.Processors
{
    /**
     * Represents the state of a single thread running within a {@link KafkaStreams} application.
     */
    public class ThreadMetadata
    {
        private readonly string threadName;
        public string ThreadState { get; }
        public HashSet<TaskMetadata> ActiveTasks { get; }
        public HashSet<TaskMetadata> standbyTasks { get; }
        private readonly string mainConsumerClientId;
        private readonly string restoreConsumerClientId;
        private readonly HashSet<string> producerClientIds;

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
            this.mainConsumerClientId = mainConsumerClientId;
            this.restoreConsumerClientId = restoreConsumerClientId;
            this.producerClientIds = producerClientIds;
            this.AdminClientId = adminClientId;
            this.threadName = threadName;
            this.ThreadState = threadState;
            this.ActiveTasks = activeTasks;
            this.standbyTasks = standbyTasks;
        }

        public string consumerClientId => mainConsumerClientId;

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

            ThreadMetadata that = (ThreadMetadata)o;
            return threadName.Equals(that.threadName)
                && ThreadState.Equals(that.ThreadState)
                && ActiveTasks.Equals(that.ActiveTasks)
                && standbyTasks.Equals(that.standbyTasks)
                && mainConsumerClientId.Equals(that.mainConsumerClientId)
                && restoreConsumerClientId.Equals(that.restoreConsumerClientId)
                && producerClientIds.Equals(that.producerClientIds)
                && AdminClientId.Equals(that.AdminClientId);
        }

        public override int GetHashCode()
        {
            // can only hash 7 things at once...
            return
            (
                (threadName, ThreadState).GetHashCode(),
                ActiveTasks,
                standbyTasks,
                mainConsumerClientId,
                restoreConsumerClientId,
                producerClientIds,
                AdminClientId
            ).GetHashCode();
        }

        public override string ToString()
        {
            return "ThreadMetadata{" +
                    $"threadName={threadName}" +
                    $", threadState={ThreadState}" +
                    $", activeTasks={ActiveTasks }" +
                    $", standbyTasks={standbyTasks}" +
                    $", consumerClientId={mainConsumerClientId}" +
                    $", restoreConsumerClientId={restoreConsumerClientId}" +
                    $", producerClientIds={producerClientIds}" +
                    $", adminClientId={AdminClientId}" +
                    '}';
        }
    }
}