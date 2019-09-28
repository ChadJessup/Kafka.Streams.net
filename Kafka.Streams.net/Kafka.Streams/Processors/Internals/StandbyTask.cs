using Confluent.Kafka;
using Kafka.Common.Metrics;
using Kafka.Streams.Processor.Internals.Metrics;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Processor.Internals
{
    /**
     * A StandbyTask
     */
    public class StandbyTask : AbstractTask
    {
        public Dictionary<TopicPartition, long> checkpointedOffsets { get; private set; } = new Dictionary<TopicPartition, long>();
        private readonly Sensor closeTaskSensor;

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
            TaskId id,
            List<TopicPartition> partitions,
            ProcessorTopology topology,
            IConsumer<byte[], byte[]> consumer,
            IChangelogReader changelogReader,
            StreamsConfig config,
            StreamsMetricsImpl metrics,
            StateDirectory stateDirectory)
            : base(id, partitions, topology, consumer, changelogReader, true, stateDirectory, config)
        {

            closeTaskSensor = metrics.threadLevelSensor("task-closed", RecordingLevel.INFO);
            processorContext = new StandbyContextImpl<byte[], byte[]>(id, config, stateMgr, metrics);
        }


        public override bool initializeStateStores()
        {
            log.LogTrace("Initializing state stores");
            registerStateStores();
            checkpointedOffsets = stateMgr.checkpointed();
            processorContext.initialize();
            taskInitialized = true;
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
            log.LogDebug("Resuming");
            updateOffsetLimits();
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
            log.LogTrace("Committing");
            flushAndCheckpointState();
            // reinitialize offset limits
            updateOffsetLimits();

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
            log.LogDebug("Suspending");
            flushAndCheckpointState();
        }

        private void flushAndCheckpointState()
        {
            stateMgr.flush();
            stateMgr.checkpoint(new Dictionary<TopicPartition, long>());
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
            closeTaskSensor.record();
            if (!taskInitialized)
            {
                return;
            }

            log.LogDebug("Closing");

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

            taskClosed = true;
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
            log.LogTrace("Updating standby replicas of its state store for partition [{}]", partition);
            long limit = stateMgr.offsetLimit(partition);

            long lastOffset = -1L;
            List<ConsumeResult<byte[], byte[]>> restoreRecords = new List<ConsumeResult<byte[], byte[]>>(records.Count);
            List<ConsumeResult<byte[], byte[]>> remainingRecords = new List<ConsumeResult<byte[], byte[]>>();

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

            stateMgr.updateStandbyStates(partition, restoreRecords, lastOffset);

            if (restoreRecords.Any())
            {
                commitNeeded = true;
            }

            return remainingRecords;
        }
    }
}