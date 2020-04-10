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

        // the admin client should be shared among All threads, so the client id should be the same;
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

        public string ConsumerClientId => this.MainConsumerClientId;

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

            var that = (ThreadMetadata)o;
            return this.ThreadName.Equals(that.ThreadName)
                && this.ThreadState.Equals(that.ThreadState)
                && this.ActiveTasks.Equals(that.ActiveTasks)
                && this.StandbyTasks.Equals(that.StandbyTasks)
                && this.MainConsumerClientId.Equals(that.MainConsumerClientId)
                && this.RestoreConsumerClientId.Equals(that.RestoreConsumerClientId)
                && this.ProducerClientIds.Equals(that.ProducerClientIds)
                && this.AdminClientId.Equals(that.AdminClientId);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                this.ThreadName,
                this.ThreadState,
                this.ActiveTasks,
                this.StandbyTasks,
                this.MainConsumerClientId,
                this.RestoreConsumerClientId,
                this.ProducerClientIds,
                this.AdminClientId);
        }

        public override string ToString()
        {
            return "ThreadMetadata{" +
                    $"threadName={this.ThreadName}" +
                    $", threadState={this.ThreadState}" +
                    $", activeTasks={this.ActiveTasks }" +
                    $", standbyTasks={this.StandbyTasks}" +
                    $", consumerClientId={this.MainConsumerClientId}" +
                    $", restoreConsumerClientId={this.RestoreConsumerClientId}" +
                    $", producerClientIds={this.ProducerClientIds}" +
                    $", adminClientId={this.AdminClientId}" +
                    '}';
        }
    }
}