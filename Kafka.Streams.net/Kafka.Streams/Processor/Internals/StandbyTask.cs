/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Confluent.Kafka;
using Kafka.Common.Metrics;
using Kafka.Streams.IProcessor.Internals.Metrics;
using System.Collections.Generic;

namespace Kafka.Streams.IProcessor.Internals
{
    /**
     * A StandbyTask
     */
    public class StandbyTask : AbstractTask
    {
        private Dictionary<TopicPartition, long> checkpointedOffsets = new Dictionary<>();
        private Sensor closeTaskSensor;

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
        StandbyTask(TaskId id,
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
            processorContext = new StandbyContextImpl(id, config, stateMgr, metrics);
        }


        public bool initializeStateStores()
        {
            log.LogTrace("Initializing state stores");
            registerStateStores();
            checkpointedOffsets = Collections.unmodifiableMap(stateMgr.checkpointed());
            processorContext.initialize();
            taskInitialized = true;
            return true;
        }


        public void initializeTopology()
        {
            //no-op
        }

        /**
         * <pre>
         * - update offset limits
         * </pre>
         */

        public void resume()
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

        public void commit()
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

        public void suspend()
        {
            log.LogDebug("Suspending");
            flushAndCheckpointState();
        }

        private void flushAndCheckpointState()
        {
            stateMgr.flush();
            stateMgr.checkpoint(Collections.emptyMap());
        }

        /**
         * <pre>
         * - {@link #commit()}
         * - close state
         * <pre>
         * @param isZombie ignored by {@code StandbyTask} as it can never be a zombie
         */

        public void close(bool clean,
                          bool isZombie)
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


        public void closeSuspended(bool clean,
                                   bool isZombie,
                                   RuntimeException e)
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
            List<ConsumeResult<byte[], byte[]>> restoreRecords = new List<>(records.size());
            List<ConsumeResult<byte[], byte[]>> remainingRecords = new List<>();

            foreach (ConsumeResult<byte[], byte[]> record in records)
            {
                if (record.offset() < limit)
                {
                    restoreRecords.Add(record);
                    lastOffset = record.offset();
                }
                else
                {

                    remainingRecords.Add(record);
                }
            }

            stateMgr.updateStandbyStates(partition, restoreRecords, lastOffset);

            if (!restoreRecords.isEmpty())
            {
                commitNeeded = true;
            }

            return remainingRecords;
        }

        Dictionary<TopicPartition, long> checkpointedOffsets()
        {
            return checkpointedOffsets;
        }

    }
}