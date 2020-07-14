using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Threads;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Tasks
{
    public abstract class AbstractTask : ITask
    {
        protected KafkaStreamsContext Context { get; }
        private TaskState state = TaskState.CREATED;

        public TaskId Id { get; }
        public abstract long LatestOffset { get; set; }
        public abstract TaskState CurrentState { get; protected set; }

        protected ProcessorTopology topology;
        protected StateDirectory stateDirectory;
        public HashSet<TopicPartition> Partitions { get; }
        protected ProcessorStateManager stateMgr;

        protected AbstractTask(
            KafkaStreamsContext context,
            TaskId id,
            ProcessorTopology topology,
            StateDirectory stateDirectory,
            ProcessorStateManager stateMgr,
            HashSet<TopicPartition> partitions)
        {
            this.Id = id;
            this.Context = context;
            this.stateMgr = stateMgr;
            this.topology = topology;
            this.Partitions = partitions;
            this.stateDirectory = stateDirectory;
        }

        public HashSet<TopicPartition> InputPartitions()
        {
            return this.Partitions;
        }

        public IEnumerable<TopicPartition> ChangelogPartitions()
        {
            return this.stateMgr.ChangelogPartitions();
        }

        public void MarkChangelogAsCorrupted(List<TopicPartition> partitions)
        {
            //stateMgr.MarkChangelogAsCorrupted(partitions);
        }

        public IStateStore GetStore(string name)
        {
            return this.stateMgr.GetStore(name);
        }


        public bool IsClosed()
        {
            return this.state == TaskState.CLOSED;
        }

        public void Revive()
        {
            if (this.state == TaskState.CLOSED)
            {
                this.TransitionTo(TaskState.CREATED);
            }
            else
            {
                throw new InvalidOperationException("Illegal state " + this.state + " while reviving task " + this.Id);
            }
        }

        protected void TransitionTo(TaskState newState)
        {
            TaskState oldState = this.state;

            if (this.IsValidTransition(oldState, newState))
            {
                this.state = newState;
                this.CurrentState = this.state;
            }
            else
            {
                throw new InvalidOperationException("Invalid transition from " + oldState + " to " + newState);
            }
        }

        protected abstract bool IsValidTransition(TaskState oldState, TaskState newState);

        protected void ExecuteAndMaybeSwallow(
            bool clean,
            Action runnable,
            string name,
            ILogger log)
        {
            if (runnable is null)
            {
                throw new ArgumentNullException(nameof(runnable));
            }

            try
            {
                runnable();
            }
            catch (RuntimeException e)
            {
                if (clean)
                {
                    throw;
                }
                else
                {
                    // log.debug("Ignoring error in unclean {}", name);
                }
            }
        }

        public abstract bool IsActive();
        public abstract void InitializeIfNeeded();
        public abstract void CompleteRestoration();
        public abstract void AddRecords(TopicPartition partition, IEnumerable<ConsumeResult<byte[], byte[]>> records);
        public abstract bool CommitNeeded();
        public abstract void PrepareCommit();
        public abstract void PostCommit();
        public abstract void PrepareSuspend();
        public abstract void Suspend();
        public abstract void Resume();
        public abstract Dictionary<TopicPartition, long> PrepareCloseClean();
        public abstract void CloseClean(Dictionary<TopicPartition, long> checkpoint);
        public abstract void PrepareCloseDirty();
        public abstract void CloseDirty();
        public abstract Dictionary<TopicPartition, long> ChangelogOffsets();
        public abstract bool SetState(TaskState state);
        public abstract void SetTransitions(IEnumerable<StateTransition<TaskState>> validTransitions);
        public abstract bool IsRunning();
        public abstract string ToString(string v);
    }
}
