using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Tasks
{
    /**
     * A StandbyTask
     */
    public class StandbyTask : AbstractTask
    {
        public Dictionary<TopicPartition, long?> checkpointedOffsets { get; private set; } = new Dictionary<TopicPartition, long?>();

        /**
         * Create {@link StandbyTask} with its assigned partitions
         *
         * @param id             the ID of this task
         * @param partitions     the collection of assigned {@link TopicPartition}
         * @param topology       the instance of {@link ProcessorTopology}
         * @param consumer       the instance of {@link Consumer}
         * @param config         the {@link StreamsConfig} specified by the user
         * @param metrics        the {@link IStreamsMetrics} created by the thread
         * @param stateDirectory the {@link StateDirectory} created by the thread
         */
        public StandbyTask(
            KafkaStreamsContext context,
            ILogger<StandbyTask> logger,
            TaskId id,
            List<TopicPartition> partitions,
            ProcessorTopology topology,
            IConsumer<byte[], byte[]> consumer,
            IChangelogReader changelogReader,
            StreamsConfig config,
            //StreamsMetricsImpl metrics,
            StateDirectory stateDirectory)
            : base(
                  context,
                  id,
                  partitions,
                  topology,
                  consumer,
                  changelogReader,
                  true,
                  stateDirectory,
                  config)
        {
            // closeTaskSensor = metrics.threadLevelSensor("task-closed", RecordingLevel.INFO);
            this.processorContext = new StandbyContextImpl<byte[], byte[]>(
                context,
                context.CreateLogger<StandbyContextImpl<byte[], byte[]>>(),
                id,
                config, this.StateMgr);
        }


        public override bool InitializeStateStores()
        {
            this.logger.LogTrace("Initializing state stores");
            this.RegisterStateStores();
            this.checkpointedOffsets = this.StateMgr.Checkpointed();
            this.processorContext.Initialize();
            this.TaskInitialized = true;
            return true;
        }

        public override void InitializeTopology()
        {
            //no-op
        }

        /**
         * <pre>
         * - update offset limits
         * </pre>
         */
        public override void Resume()
        {
            this.logger.LogDebug("Resuming");
            this.UpdateOffsetLimits();
        }

        /**
         * <pre>
         * - Flush store
         * - checkpoint store
         * - update offset limits
         * </pre>
         */

        public override void Commit()
        {
            this.logger.LogTrace("Committing");
            this.FlushAndCheckpointState();
            // reinitialize offset limits
            this.UpdateOffsetLimits();

            this.commitNeeded = false;
        }

        /**
         * <pre>
         * - Flush store
         * - checkpoint store
         * </pre>
         */
        public override void Suspend()
        {
            this.logger.LogDebug("Suspending");
            this.FlushAndCheckpointState();
        }

        private void FlushAndCheckpointState()
        {
            this.StateMgr.Flush();
            this.StateMgr.Checkpoint(new Dictionary<TopicPartition, long>());
        }

        /**
         * <pre>
         * - {@link #commit()}
         * - Close state
         * <pre>
         * @param isZombie ignored by {@code StandbyTask} as it can never be a zombie
         */

        public override void Close(bool clean, bool isZombie)
        {
            if (!this.TaskInitialized)
            {
                return;
            }

            this.logger.LogDebug("Closing");

            try
            {

                if (clean)
                {
                    this.Commit();
                }
            }
            finally
            {

                this.CloseStateManager(true);
            }

            this.TaskClosed = true;
        }


        public override void CloseSuspended(bool clean, bool isZombie, RuntimeException e)
        {
            this.Close(clean, isZombie);
        }

        /**
         * Updates a state store using records from one change log partition
         *
         * @return a list of records not consumed
         */
        public List<ConsumeResult<byte[], byte[]>> Update(
            TopicPartition partition,
            List<ConsumeResult<byte[], byte[]>> records)
        {
            this.logger.LogTrace("Updating standby replicas of its state store for partition [{}]", partition);
            var limit = this.StateMgr.OffsetLimit(partition);

            var lastOffset = -1L;
            var restoreRecords = new List<ConsumeResult<byte[], byte[]>>(records.Count);
            var remainingRecords = new List<ConsumeResult<byte[], byte[]>>();

            foreach (ConsumeResult<byte[], byte[]> record in records)
            {
                if (record.Offset < limit)
                {
                    restoreRecords.Add(record);
                    lastOffset = record.Offset;
                }
                else
                {

                    remainingRecords.Add(record);
                }
            }

            this.StateMgr.UpdateStandbyStates(partition, restoreRecords, lastOffset);

            if (restoreRecords.Any())
            {
                this.commitNeeded = true;
            }

            return remainingRecords;
        }

        public override void InitializeIfNeeded()
        {
            throw new System.NotImplementedException();
        }

        public override void CompleteRestoration()
        {
            throw new System.NotImplementedException();
        }
    }
}
