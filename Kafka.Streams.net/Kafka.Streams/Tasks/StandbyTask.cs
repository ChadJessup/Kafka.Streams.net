using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Threads;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Tasks
{
    /**
     * A StandbyTask
     */
    public class StandbyTask : AbstractTask
    {
        private readonly ILogger<StandbyTask> logger;
        private readonly string logPrefix;
        private readonly bool eosEnabled;
        private readonly IInternalProcessorContext processorContext;

        private Dictionary<TopicPartition, long> offsetSnapshotSinceLastCommit;

        public override long LatestOffset { get; set; }
        public override TaskState CurrentState { get; protected set; }
        public Dictionary<TopicPartition, long> CheckpointedOffsets { get; internal set; }

        /**
         * @param id             the ID of this task
         * @param partitions     input topic partitions, used for thread metadata only
         * @param topology       the instance of {@link ProcessorTopology}
         * @param config         the {@link StreamsConfig} specified by the user
         * @param metrics        the {@link StreamsMetrics} created by the thread
         * @param stateMgr       the {@link ProcessorStateManager} for this task
         * @param stateDirectory the {@link StateDirectory} created by the thread
         */
        public StandbyTask(
            KafkaStreamsContext context,
            TaskId id,
            HashSet<TopicPartition> partitions,
            ProcessorTopology topology,
            StreamsConfig config,
            ProcessorStateManager stateMgr,
            StateDirectory stateDirectory)
            : base(
                  context,
                  id,
                  topology,
                  stateDirectory,
                  stateMgr,
                  partitions)
        {
            this.logger = this.Context.CreateLogger<StandbyTask>();
            string threadIdPrefix = $"stream-thread [{Thread.CurrentThread.Name}] ";
            this.logPrefix = threadIdPrefix + $"standby-task [{id}] ";
            //LogContext logContext = new LogContext(logPrefix);
            //log = logContext.logger(getClass());

            this.processorContext = null; // new StandbyContextImpl(id, config, stateMgr);
            // eosEnabled = StreamThread.eosEnabled(config);
        }

        public override bool IsActive()
        {
            return false;
        }

        /**
         * @throws StreamsException fatal error, should close the thread
         */
        public override void InitializeIfNeeded()
        {
            if (this.CurrentState == TaskState.CREATED)
            {
                StateManagerUtil.RegisterStateStores(this.logger, this.logPrefix, this.topology, this.stateMgr, this.stateDirectory, this.processorContext);

                // no topology needs initialized, we can transit to RUNNING
                // right after registered the stores
                this.TransitionTo(TaskState.RESTORING);
                this.TransitionTo(TaskState.RUNNING);

                this.processorContext.Initialize();

                this.logger.LogInformation("Initialized");
            }
        }

        public override void CompleteRestoration()
        {
            throw new InvalidOperationException("Standby task " + this.Id + " should never be completing restoration");
        }

        public override void PrepareSuspend()
        {
            this.logger.LogTrace("No-op prepareSuspend with state {}", this.CurrentState);
        }

        public override void Suspend()
        {
            this.logger.LogTrace("No-op suspend with state {}", this.CurrentState);
        }

        public override void Resume()
        {
            this.logger.LogTrace("No-op resume with state {}", this.CurrentState);
        }

        /**
         * 1. flush store
         * 2. write checkpoint file
         *
         * @throws StreamsException fatal error, should close the thread
         */
        public override void PrepareCommit()
        {
            if (this.CurrentState == TaskState.RUNNING)
            {
                this.stateMgr.Flush();
                this.logger.LogInformation("Task ready for committing");
            }
            else
            {
                throw new InvalidOperationException("Illegal state " + this.CurrentState + " while preparing standby task " + this.Id + " for committing ");
            }
        }

        public override void PostCommit()
        {
            if (this.CurrentState == TaskState.RUNNING)
            {
                // since there's no written offsets we can checkpoint with empty map,
                // and the state current offset would be used to checkpoint
                this.stateMgr.Checkpoint(new Dictionary<TopicPartition, long>());
                this.offsetSnapshotSinceLastCommit = this.stateMgr.ChangelogOffsets();
                this.logger.LogInformation("Finalized commit");
            }
            else
            {
                throw new InvalidOperationException("Illegal state " + this.CurrentState + " while post committing standby task " + this.Id);
            }
        }

        public override Dictionary<TopicPartition, long> PrepareCloseClean()
        {
            this.PrepareClose(true);

            this.logger.LogInformation("Prepared clean close");

            return new Dictionary<TopicPartition, long>();
        }

        public override void PrepareCloseDirty()
        {
            this.PrepareClose(false);

            this.logger.LogInformation("Prepared dirty close");
        }

        /**
         * 1. commit if we are running and clean close;
         * 2. close the state manager.
         *
         * @throws TaskMigratedException all the task has been migrated
         * @throws StreamsException fatal error, should close the thread
         */
        private void PrepareClose(bool clean)
        {
            if (this.CurrentState == TaskState.CREATED)
            {
                // the task is created and not initialized, do nothing
                return;
            }

            if (this.CurrentState == TaskState.RUNNING)
            {
                if (clean)
                {
                    this.stateMgr.Flush();
                }
            }
            else
            {
                throw new InvalidOperationException("Illegal state " + this.CurrentState + " while closing standby task " + this.Id);
            }
        }

        public override void CloseClean(Dictionary<TopicPartition, long> checkpoint)
        {
            if (checkpoint is null)
            {
                throw new ArgumentNullException(nameof(checkpoint));
            }

            this.Close(true);

            this.logger.LogInformation("Closed clean");
        }

        public override void CloseDirty()
        {
            this.Close(false);

            this.logger.LogInformation("Closed dirty");
        }

        private void Close(bool clean)
        {
            if (this.CurrentState == TaskState.CREATED || this.CurrentState == TaskState.RUNNING)
            {
                if (clean)
                {
                    // since there's no written offsets we can checkpoint with empty map,
                    // and the state current offset would be used to checkpoint
                    this.stateMgr.Checkpoint(new Dictionary<TopicPartition, long>());
                    this.offsetSnapshotSinceLastCommit = new Dictionary<TopicPartition, long>();// stateMgr.ChangelogOffsets);
                }

                this.ExecuteAndMaybeSwallow(clean, () =>
                    StateManagerUtil.CloseStateManager(
                        this.logger,
                        this.logPrefix,
                        clean,
                        this.eosEnabled,
                        this.stateMgr,
                        this.stateDirectory,
                        TaskType.STANDBY),
                    "state manager close",
                    this.logger);
            }
            else
            {
                throw new InvalidOperationException("Illegal state " + this.CurrentState + " while closing standby task " + this.Id);
            }

            //closeTaskSensor.record();
            this.TransitionTo(TaskState.CLOSED);
        }

        public override bool CommitNeeded()
        {
            // we can commit if the store's offset has changed since last commit
            return this.offsetSnapshotSinceLastCommit == null
                || !this.offsetSnapshotSinceLastCommit.Equals(this.stateMgr.ChangelogOffsets());
        }

        public override Dictionary<TopicPartition, long> ChangelogOffsets()
        {
            return this.stateMgr.ChangelogOffsets();
        }

        public override void AddRecords(TopicPartition partition, IEnumerable<ConsumeResult<byte[], byte[]>> records)
        {
            throw new InvalidOperationException("Attempted to add records to task " + this.Id + " for invalid input partition " + partition);
        }

        /**
         * Produces a string representation containing useful information about a Task.
         * This is useful in debugging scenarios.
         *
         * @return A string representation of the StreamTask instance.
         */
        public override string ToString()
        {
            return this.ToString("");
        }

        /**
         * Produces a string representation containing useful information about a Task starting with the given indent.
         * This is useful in debugging scenarios.
         *
         * @return A string representation of the Task instance.
         */
        public override string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(indent);
            sb.Append("TaskId: ");
            sb.Append(this.Id);
            sb.Append("\n");

            // print topology
            if (this.topology != null)
            {
                sb.Append(indent).Append(this.topology.ToString(indent + "\t"));
            }

            return sb.ToString();
        }

        public override bool SetState(TaskState state)
        {
            throw new NotImplementedException();
        }

        public override void SetTransitions(IEnumerable<StateTransition<TaskState>> validTransitions)
        {
            throw new NotImplementedException();
        }

        public override bool IsRunning()
        {
            throw new NotImplementedException();
        }

        protected override bool IsValidTransition(TaskState oldState, TaskState newState)
        {
            throw new NotImplementedException();
        }

        internal List<ConsumeResult<byte[], byte[]>> Update(TopicPartition partition, List<ConsumeResult<byte[], byte[]>> remaining)
        {
            throw new NotImplementedException();
        }

        internal void ReinitializeStateStoresForPartitions(List<TopicPartition> lists)
        {
            throw new NotImplementedException();
        }
    }
}
