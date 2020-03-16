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
            ILoggerFactory loggerFactory,
            ILogger<StandbyTask> logger,
            TaskId id,
            List<TopicPartition> partitions,
            ProcessorTopology topology,
            IConsumer<byte[], byte[]> consumer,
            IChangelogReader changelogReader,
            StreamsConfig config,
            //StreamsMetricsImpl metrics,
            StateDirectory stateDirectory)
            : base(id, partitions, topology, consumer, changelogReader, true, stateDirectory, config)
        {

            // closeTaskSensor = metrics.threadLevelSensor("task-closed", RecordingLevel.INFO);
            processorContext = new StandbyContextImpl<byte[], byte[]>(loggerFactory, loggerFactory.CreateLogger<StandbyContextImpl<byte[], byte[]>>(), id, config, StateMgr);//, metrics);
        }


        public override bool initializeStateStores()
        {
            logger.LogTrace("Initializing state stores");
            registerStateStores();
            checkpointedOffsets = StateMgr.checkpointed();
            processorContext.initialize();
            TaskInitialized = true;
            return true;
        }


        public override void initializeTopology()
        {
            //no-op
        }

        /**
         * <pre>
         * - update offset limits
         * </pre>
         */
        public override void resume()
        {
            logger.LogDebug("Resuming");
            UpdateOffsetLimits();
        }

        /**
         * <pre>
         * - flush store
         * - checkpoint store
         * - update offset limits
         * </pre>
         */

        public override void commit()
        {
            logger.LogTrace("Committing");
            flushAndCheckpointState();
            // reinitialize offset limits
            UpdateOffsetLimits();

            commitNeeded = false;
        }

        /**
         * <pre>
         * - flush store
         * - checkpoint store
         * </pre>
         */
        public override void suspend()
        {
            logger.LogDebug("Suspending");
            flushAndCheckpointState();
        }

        private void flushAndCheckpointState()
        {
            StateMgr.Flush();
            StateMgr.checkpoint(new Dictionary<TopicPartition, long>());
        }

        /**
         * <pre>
         * - {@link #commit()}
         * - close state
         * <pre>
         * @param isZombie ignored by {@code StandbyTask} as it can never be a zombie
         */

        public override void close(bool clean, bool isZombie)
        {
            if (!TaskInitialized)
            {
                return;
            }

            logger.LogDebug("Closing");

            try
            {

                if (clean)
                {
                    commit();
                }
            }
            finally
            {

                closeStateManager(true);
            }

            TaskClosed = true;
        }


        public override void closeSuspended(bool clean, bool isZombie, RuntimeException e)
        {
            close(clean, isZombie);
        }

        /**
         * Updates a state store using records from one change log partition
         *
         * @return a list of records not consumed
         */
        public List<ConsumeResult<byte[], byte[]>> update(TopicPartition partition,
                                                           List<ConsumeResult<byte[], byte[]>> records)
        {
            logger.LogTrace("Updating standby replicas of its state store for partition [{}]", partition);
            var limit = StateMgr.offsetLimit(partition);

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

            StateMgr.updateStandbyStates(partition, restoreRecords, lastOffset);

            if (restoreRecords.Any())
            {
                commitNeeded = true;
            }

            return remainingRecords;
        }

        public override void initializeIfNeeded()
        {
            throw new System.NotImplementedException();
        }

        public override void CompleteRestoration()
        {
            throw new System.NotImplementedException();
        }
    }
}