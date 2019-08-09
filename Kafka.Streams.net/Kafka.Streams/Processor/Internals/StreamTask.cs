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
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    /**
     * A StreamTask is associated with a {@link PartitionGroup}, and is assigned to a StreamThread for processing.
     */
    public partial class StreamTask : AbstractTask, IProcessorNodePunctuator
    {
        private static ConsumeResult<object, object> DUMMY_RECORD = new ConsumeResult<>(ProcessorContextImpl.NONEXIST_TOPIC, -1, -1L, null, null);

        private ITime time;
        private long maxTaskIdleMs;
        private int maxBufferedSize;
        private TaskMetrics taskMetrics;
        private PartitionGroup partitionGroup;
        private RecordCollector recordCollector;
        private PartitionGroup.RecordInfo recordInfo;
        private Dictionary<TopicPartition, long> consumedOffsets;
        private PunctuationQueue streamTimePunctuationQueue;
        private PunctuationQueue systemTimePunctuationQueue;
        private IProducerSupplier producerSupplier;

        private Sensor closeTaskSensor;
        private long idleStartTime;
        private IProducer<byte[], byte[]> producer;
        private bool commitRequested = false;
        private bool transactionInFlight = false;

        public StreamTask(
            TaskId id,
            List<TopicPartition> partitions,
            ProcessorTopology topology,
            IConsumer<byte[], byte[]> consumer,
            ChangelogReader changelogReader,
            StreamsConfig config,
            StreamsMetricsImpl metrics,
            StateDirectory stateDirectory,
            ThreadCache cache,
            ITime time,
            IProducerSupplier producerSupplier)
            : this(id, partitions, topology, consumer, changelogReader, config, metrics, stateDirectory, cache, time, producerSupplier, null)
        {
        }

        public StreamTask(TaskId id,
                          List<TopicPartition> partitions,
                          ProcessorTopology topology,
                          IConsumer<byte[], byte[]> consumer,
                          ChangelogReader changelogReader,
                          StreamsConfig config,
                          StreamsMetricsImpl streamsMetrics,
                          StateDirectory stateDirectory,
                          ThreadCache cache,
                          ITime time,
                          IProducerSupplier producerSupplier,
                          RecordCollector recordCollector)
            : base(id, partitions, topology, consumer, changelogReader, false, stateDirectory, config)
        {

            this.time = time;
            this.producerSupplier = producerSupplier;
            this.producer = producerSupplier[];
            this.taskMetrics = new TaskMetrics(id, streamsMetrics);

            closeTaskSensor = ThreadMetrics.closeTaskSensor(streamsMetrics);

            ProductionExceptionHandler productionExceptionHandler = config.defaultProductionExceptionHandler();

            if (recordCollector == null)
            {
                this.recordCollector = new RecordCollectorImpl(
                    id.ToString(),
                    logContext,
                    productionExceptionHandler,
                    ThreadMetrics.skipRecordSensor(streamsMetrics));
            }
            else
            {

                this.recordCollector = recordCollector;
            }
            this.recordCollector.init(this.producer);

            streamTimePunctuationQueue = new PunctuationQueue();
            systemTimePunctuationQueue = new PunctuationQueue();
            maxTaskIdleMs = config.getLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG);
            maxBufferedSize = config.getInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG);

            // initialize the consumed and committed offset cache
            consumedOffsets = new Dictionary<>();

            // create queues for each assigned partition and associate them
            // to corresponding source nodes in the processor topology
            Dictionary<TopicPartition, RecordQueue> partitionQueues = new Dictionary<>();

            // initialize the topology with its own context
            ProcessorContextImpl processorContextImpl = new ProcessorContextImpl(id, this, config, this.recordCollector, stateMgr, streamsMetrics, cache);
            processorContext = processorContextImpl;

            TimestampExtractor defaultTimestampExtractor = config.defaultTimestampExtractor();
            DeserializationExceptionHandler defaultDeserializationExceptionHandler = config.defaultDeserializationExceptionHandler();
            foreach (TopicPartition partition in partitions)
            {
                SourceNode source = topology.source(partition.Topic);
                TimestampExtractor sourceTimestampExtractor = source.getTimestampExtractor() != null ? source.getTimestampExtractor() : defaultTimestampExtractor;
                RecordQueue queue = new RecordQueue(
                    partition,
                    source,
                    sourceTimestampExtractor,
                    defaultDeserializationExceptionHandler,
                    processorContext,
                    logContext
                );
                partitionQueues.Add(partition, queue);
            }

            recordInfo = new PartitionGroup.RecordInfo();
            partitionGroup = new PartitionGroup(partitionQueues, recordLatenessSensor(processorContextImpl));

            stateMgr.registerGlobalStateStores(topology.globalStateStores());

            // initialize transactions if eos is turned on, which will block if the previous transaction has not
            // completed yet; do not start the first transaction until the topology has been initialized later
            if (eosEnabled)
            {
                initializeTransactions();
            }
        }


        public bool initializeStateStores()
        {
            log.LogTrace("Initializing state stores");
            registerStateStores();

            return changelogPartitions().isEmpty();
        }

        /**
         * <pre>
         * - (re-)initialize the topology of the task
         * </pre>
         *
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */

        public void initializeTopology()
        {
            initTopology();

            if (eosEnabled)
            {
                try
                {

                    this.producer.beginTransaction();
                }
                catch (ProducerFencedException fatal)
                {
                    throw new TaskMigratedException(this, fatal);
                }
                transactionInFlight = true;
            }

            processorContext.initialize();

            taskInitialized = true;

            idleStartTime = RecordQueue.UNKNOWN;

            stateMgr.ensureStoresRegistered();
        }

        /**
         * <pre>
         * - resume the task
         * </pre>
         */

        public void resume()
        {
            log.LogDebug("Resuming");
            if (eosEnabled)
            {
                if (producer != null)
                {
                    throw new InvalidOperationException("Task producer should be null.");
                }
                producer = producerSupplier[];
                initializeTransactions();
                recordCollector.init(producer);

                try
                {

                    stateMgr.clearCheckpoints();
                }
                catch (IOException e)
                {
                    throw new ProcessorStateException(format("%sError while deleting the checkpoint file", logPrefix), e);
                }
            }
        }

        /**
         * An active task is processable if its buffer contains data for all of its input
         * source topic partitions, or if it is enforced to be processable
         */
        bool isProcessable(long now)
        {
            if (partitionGroup.allPartitionsBuffered())
            {
                idleStartTime = RecordQueue.UNKNOWN;
                return true;
            }
            else if (partitionGroup.numBuffered() > 0)
            {
                if (idleStartTime == RecordQueue.UNKNOWN)
                {
                    idleStartTime = now;
                }

                if (now - idleStartTime >= maxTaskIdleMs)
                {
                    taskMetrics.taskEnforcedProcessSensor.record();
                    return true;
                }
                else
                {

                    return false;
                }
            }
            else
            {

                return false;
            }
        }

        /**
         * Process one record.
         *
         * @return true if this method processes a record, false if it does not process a record.
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */

        public bool process()
        {
            // get the next record to process
            StampedRecord record = partitionGroup.nextRecord(recordInfo);

            // if there is no record to process, return immediately
            if (record == null)
            {
                return false;
            }

            try
            {

                // process the record by passing to the source node of the topology
                ProcessorNode currNode = recordInfo.node();
                TopicPartition partition = recordInfo.partition();

                log.LogTrace("Start processing one record [{}]", record);

                updateProcessorContext(record, currNode);
                currNode.process(record.key(), record.value());

                log.LogTrace("Completed processing one record [{}]", record);

                // update the consumed offset map after processing is done
                consumedOffsets.Add(partition, record.offset());
                commitNeeded = true;

                // after processing this record, if its partition queue's buffered size has been
                // decreased to the threshold, we can then resume the consumption on this partition
                if (recordInfo.queue().size() == maxBufferedSize)
                {
                    consumer.resume(partition);
                }
            }
            catch (ProducerFencedException fatal)
            {
                throw new TaskMigratedException(this, fatal);
            }
            catch (KafkaException e)
            {
                string stackTrace = getStacktraceString(e);
                throw new StreamsException(format("Exception caught in process. taskId=%s, " +
                        "processor=%s, topic=%s, partition=%d, offset=%d, stacktrace=%s",
                    id(),
                    processorContext.currentNode().name(),
                    record.Topic,
                    record.partition(),
                    record.offset(),
                    stackTrace
                ), e);
            }
            finally
            {

                processorContext.setCurrentNode(null);
            }

            return true;
        }

        private string getStacktraceString(KafkaException e)
        {
            string stacktrace = null;
            try (StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter))
{
                e.printStackTrace(printWriter);
                stacktrace = stringWriter.ToString();
            } catch (IOException ioe)
            {
                log.LogError("Encountered error extracting stacktrace from this exception", ioe);
            }
            return stacktrace;
        }

        /**
         * @throws InvalidOperationException if the current node is not null
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */

        public void punctuate(ProcessorNode node, long timestamp, PunctuationType type, Punctuator punctuator)
        {
            if (processorContext.currentNode() != null)
            {
                throw new InvalidOperationException(string.Format("%sCurrent node is not null", logPrefix));
            }

            updateProcessorContext(new StampedRecord(DUMMY_RECORD, timestamp), node);

            if (log.isTraceEnabled())
            {
                log.LogTrace("Punctuating processor {} with timestamp {} and punctuation type {}", node.name(), timestamp, type);
            }

            try
            {

                node.punctuate(timestamp, punctuator);
            }
            catch (ProducerFencedException fatal)
            {
                throw new TaskMigratedException(this, fatal);
            }
            catch (KafkaException e)
            {
                throw new StreamsException(string.Format("%sException caught while punctuating processor '%s'", logPrefix, node.name()), e);
            }
            finally
            {

                processorContext.setCurrentNode(null);
            }
        }

        private void updateProcessorContext(StampedRecord record, ProcessorNode currNode)
        {
            processorContext.setRecordContext(
                new ProcessorRecordContext(
                    record.timestamp,
                    record.offset(),
                    record.partition(),
                    record.Topic,
                    record.headers()));
            processorContext.setCurrentNode(currNode);
        }

        /**
         * <pre>
         * - flush state and producer
         * - if(!eos) write checkpoint
         * - commit offsets and start new transaction
         * </pre>
         *
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */

        public void commit()
        {
            commit(true);
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        // visible for testing
        void commit(bool startNewTransaction)
        {
            long startNs = time.nanoseconds();
            log.LogDebug("Committing");

            flushState();

            if (!eosEnabled)
            {
                stateMgr.checkpoint(activeTaskCheckpointableOffsets());
            }

            Dictionary<TopicPartition, OffsetAndMetadata> consumedOffsetsAndMetadata = new Dictionary<>(consumedOffsets.size());
            foreach (KeyValuePair<TopicPartition, long> entry in consumedOffsets)
            {
                TopicPartition partition = entry.Key;
                long offset = entry.Value + 1;
                consumedOffsetsAndMetadata.Add(partition, new OffsetAndMetadata(offset));
                stateMgr.putOffsetLimit(partition, offset);
            }

            try
            {

                if (eosEnabled)
                {
                    producer.sendOffsetsToTransaction(consumedOffsetsAndMetadata, applicationId);
                    producer.commitTransaction();
                    transactionInFlight = false;
                    if (startNewTransaction)
                    {
                        producer.beginTransaction();
                        transactionInFlight = true;
                    }
                }
                else
                {

                    consumer.commitSync(consumedOffsetsAndMetadata);
                }
            }
            catch (CommitFailedException | ProducerFencedException error)
{
                throw new TaskMigratedException(this, error);
            }

            commitNeeded = false;
            commitRequested = false;
            taskMetrics.taskCommitTimeSensor.record(time.nanoseconds() - startNs);
            }


            protected Dictionary<TopicPartition, long> activeTaskCheckpointableOffsets()
            {
                Dictionary<TopicPartition, long> checkpointableOffsets = recordCollector.offsets();
                foreach (KeyValuePair<TopicPartition, long> entry in consumedOffsets)
                {
                    checkpointableOffsets.putIfAbsent(entry.Key, entry.Value);
                }

                return checkpointableOffsets;
            }


            protected void flushState()
            {
                log.LogTrace("Flushing state and producer");
                base.flushState();
                try
                {

                    recordCollector.flush();
                }
                catch (ProducerFencedException fatal)
                {
                    throw new TaskMigratedException(this, fatal);
                }
            }

            Dictionary<TopicPartition, long> purgableOffsets()
            {
                Dictionary<TopicPartition, long> purgableConsumedOffsets = new Dictionary<>();
                foreach (KeyValuePair<TopicPartition, long> entry in consumedOffsets)
                {
                    TopicPartition tp = entry.Key;
                    if (topology.isRepartitionTopic(tp.Topic))
                    {
                        purgableConsumedOffsets.Add(tp, entry.Value + 1);
                    }
                }

                return purgableConsumedOffsets;
            }

            private void initTopology()
            {
                // initialize the task by initializing all its processor nodes in the topology
                log.LogTrace("Initializing processor nodes of the topology");
                foreach (ProcessorNode node in topology.processors())
                {
                    processorContext.setCurrentNode(node);
                    try
                    {

                        node.init(processorContext);
                    }
                    finally
                    {

                        processorContext.setCurrentNode(null);
                    }
                }
            }

            /**
             * <pre>
             * - close topology
             * - {@link #commit()}
             *   - flush state and producer
             *   - if (!eos) write checkpoint
             *   - commit offsets
             * </pre>
             *
             * @throws TaskMigratedException if committing offsets failed (non-EOS)
             *                               or if the task producer got fenced (EOS)
             */

            public void suspend()
            {
                log.LogDebug("Suspending");
                suspend(true, false);
            }

            /**
             * <pre>
             * - close topology
             * - if (clean) {@link #commit()}
             *   - flush state and producer
             *   - if (!eos) write checkpoint
             *   - commit offsets
             * </pre>
             *
             * @throws TaskMigratedException if committing offsets failed (non-EOS)
             *                               or if the task producer got fenced (EOS)
             */
            // visible for testing
            void suspend(bool clean,
                         bool isZombie)
            {
                try
                {

                    closeTopology(); // should we call this only on clean suspend?
                }
                catch (RuntimeException fatal)
                {
                    if (clean)
                    {
                        throw fatal;
                    }
                }

                if (clean)
                {
                    TaskMigratedException taskMigratedException = null;
                    try
                    {

                        commit(false);
                    }
                    finally
                    {

                        if (eosEnabled)
                        {

                            stateMgr.checkpoint(activeTaskCheckpointableOffsets());

                            try
                            {

                                recordCollector.close();
                            }
                            catch (ProducerFencedException e)
                            {
                                taskMigratedException = new TaskMigratedException(this, e);
                            }
                            finally
                            {

                                producer = null;
                            }
                        }
                    }
                    if (taskMigratedException != null)
                    {
                        throw taskMigratedException;
                    }
                }
                else
                {

                    maybeAbortTransactionAndCloseRecordCollector(isZombie);
                }
            }

            private void maybeAbortTransactionAndCloseRecordCollector(bool isZombie)
            {
                if (eosEnabled && !isZombie)
                {
                    try
                    {

                        if (transactionInFlight)
                        {
                            producer.abortTransaction();
                        }
                        transactionInFlight = false;
                    }
                    catch (ProducerFencedException ignore)
                    {
                        /* TODO
                         * this should actually never happen atm as we guard the call to #abortTransaction
                         * => the reason for the guard is a "bug" in the Producer -- it throws InvalidOperationException
                         * instead of ProducerFencedException atm. We can Remove the isZombie flag after KAFKA-5604 got
                         * fixed and fall-back to this catch-and-swallow code
                         */

                        // can be ignored: transaction got already aborted by brokers/transactional-coordinator if this happens
                    }
                }

                if (eosEnabled)
                {
                    try
                    {

                        recordCollector.close();
                    }
                    catch (Throwable e)
                    {
                        log.LogError("Failed to close producer due to the following error:", e);
                    }
                    finally
                    {

                        producer = null;
                    }
                }
            }

            private void closeTopology()
            {
                log.LogTrace("Closing processor topology");

                partitionGroup.clear();

                // close the processors
                // make sure close() is called for each node even when there is a RuntimeException
                RuntimeException exception = null;
                if (taskInitialized)
                {
                    foreach (ProcessorNode node in topology.processors())
                    {
                        processorContext.setCurrentNode(node);
                        try
                        {

                            node.close();
                        }
                        catch (RuntimeException e)
                        {
                            exception = e;
                        }
                        finally
                        {

                            processorContext.setCurrentNode(null);
                        }
                    }
                }

                if (exception != null)
                {
                    throw exception;
                }
            }

            // helper to avoid calling suspend() twice if a suspended task is not reassigned and closed

            public void closeSuspended(bool clean,
                                       bool isZombie,
                                       RuntimeException firstException)
            {
                try
                {

                    closeStateManager(clean);
                }
                catch (RuntimeException e)
                {
                    if (firstException == null)
                    {
                        firstException = e;
                    }
                    log.LogError("Could not close state manager due to the following error:", e);
                }

                partitionGroup.close();
                taskMetrics.removeAllSensors();

                closeTaskSensor.record();

                if (firstException != null)
                {
                    throw firstException;
                }
            }

            /**
             * <pre>
             * - {@link #suspend(bool, bool) suspend(clean)}
             *   - close topology
             *   - if (clean) {@link #commit()}
             *     - flush state and producer
             *     - commit offsets
             * - close state
             *   - if (clean) write checkpoint
             * - if (eos) close producer
             * </pre>
             *
             * @param clean    shut down cleanly (ie, incl. flush and commit) if {@code true} --
             *                 otherwise, just close open resources
             * @param isZombie {@code true} is this task is a zombie or not (this will repress {@link TaskMigratedException}
             * @throws TaskMigratedException if committing offsets failed (non-EOS)
             *                               or if the task producer got fenced (EOS)
             */

            public void close(bool clean,
                              bool isZombie)
            {
                log.LogDebug("Closing");

                RuntimeException firstException = null;
                try
                {

                    suspend(clean, isZombie);
                }
                catch (RuntimeException e)
                {
                    clean = false;
                    firstException = e;
                    log.LogError("Could not close task due to the following error:", e);
                }

                closeSuspended(clean, isZombie, firstException);

                taskClosed = true;
            }

            /**
             * Adds records to queues. If a record has an invalid (i.e., negative) timestamp, the record is skipped
             * and not.Added to the queue for processing
             *
             * @param partition the partition
             * @param records   the records
             */
            public void addRecords(TopicPartition partition, IEnumerable<ConsumeResult<byte[], byte[]>> records)
            {
                int newQueueSize = partitionGroup.AddRawRecords(partition, records);

                if (log.isTraceEnabled())
                {
                    log.LogTrace("Added records into the buffered queue of partition {}, new queue size is {}", partition, newQueueSize);
                }

                // if after.Adding these records, its partition queue's buffered size has been
                // increased beyond the threshold, we can then pause the consumption for this partition
                if (newQueueSize > maxBufferedSize)
                {
                    consumer.pause(singleton(partition));
                }
            }

            /**
             * Schedules a punctuation for the processor
             *
             * @param interval the interval in milliseconds
             * @param type     the punctuation type
             * @throws InvalidOperationException if the current node is not null
             */
            public ICancellable schedule(long interval, PunctuationType type, Punctuator punctuator)
            {
                switch (type)
                {
                    case STREAM_TIME:
                        // align punctuation to 0L, punctuate as soon as we have data
                        return schedule(0L, interval, type, punctuator);
                    case WALL_CLOCK_TIME:
                        // align punctuation to now, punctuate after interval has elapsed
                        return schedule(time.milliseconds() + interval, interval, type, punctuator);
                    default:
                        throw new System.ArgumentException("Unrecognized PunctuationType: " + type);
                }
            }

            /**
             * Schedules a punctuation for the processor
             *
             * @param startTime time of the first punctuation
             * @param interval  the interval in milliseconds
             * @param type      the punctuation type
             * @throws InvalidOperationException if the current node is not null
             */
            ICancellable schedule(long startTime, long interval, PunctuationType type, Punctuator punctuator)
            {
                if (processorContext.currentNode() == null)
                {
                    throw new InvalidOperationException(string.Format("%sCurrent node is null", logPrefix));
                }

                PunctuationSchedule schedule = new PunctuationSchedule(processorContext.currentNode(), startTime, interval, punctuator);

                switch (type)
                {
                    case STREAM_TIME:
                        // STREAM_TIME punctuation is data driven, will first punctuate as soon as stream-time is known and >= time,
                        // stream-time is known when we have received at least one record from each input topic
                        return streamTimePunctuationQueue.schedule(schedule);
                    case WALL_CLOCK_TIME:
                        // WALL_CLOCK_TIME is driven by the wall clock time, will first punctuate when now >= time
                        return systemTimePunctuationQueue.schedule(schedule);
                    default:
                        throw new System.ArgumentException("Unrecognized PunctuationType: " + type);
                }
            }

            /**
             * @return The number of records left in the buffer of this task's partition group
             */
            int numBuffered()
            {
                return partitionGroup.numBuffered();
            }

            /**
             * Possibly trigger registered stream-time punctuation functions if
             * current partition group timestamp has reached the defined stamp
             * Note, this is only called in the presence of new records
             *
             * @throws TaskMigratedException if the task producer got fenced (EOS only)
             */
            public bool maybePunctuateStreamTime()
            {
                long streamTime = partitionGroup.streamTime();

                // if the timestamp is not known yet, meaning there is not enough data accumulated
                // to reason stream partition time, then skip.
                if (streamTime == RecordQueue.UNKNOWN)
                {
                    return false;
                }
                else
                {

                    bool punctuated = streamTimePunctuationQueue.mayPunctuate(streamTime, PunctuationType.STREAM_TIME, this);

                    if (punctuated)
                    {
                        commitNeeded = true;
                    }

                    return punctuated;
                }
            }

            /**
             * Possibly trigger registered system-time punctuation functions if
             * current system timestamp has reached the defined stamp
             * Note, this is called irrespective of the presence of new records
             *
             * @throws TaskMigratedException if the task producer got fenced (EOS only)
             */
            public bool maybePunctuateSystemTime()
            {
                long systemTime = time.milliseconds();

                bool punctuated = systemTimePunctuationQueue.mayPunctuate(systemTime, PunctuationType.WALL_CLOCK_TIME, this);

                if (punctuated)
                {
                    commitNeeded = true;
                }

                return punctuated;
            }

            /**
             * Request committing the current task's state
             */
            void requestCommit()
            {
                commitRequested = true;
            }

            /**
             * Whether or not a request has been made to commit the current state
             */
            bool commitRequested()
            {
                return commitRequested;
            }

            // visible for testing only
            RecordCollector recordCollector()
            {
                return recordCollector;
            }

            IProducer<byte[], byte[]> getProducer()
            {
                return producer;
            }

            private void initializeTransactions()
            {
                try
                {

                    producer.initTransactions();
                }
                catch (TimeoutException retriable)
                {
                    log.LogError(
                        "Timeout exception caught when initializing transactions for task {}. " +
                            "This might happen if the broker is slow to respond, if the network connection to " +
                            "the broker was interrupted, or if similar circumstances arise. " +
                            "You can increase producer parameter `max.block.ms` to increase this timeout.",
                        id,
                        retriable
                    );
                    throw new StreamsException(
                        format("%sFailed to initialize task %s due to timeout.", logPrefix, id),
                        retriable
                    );
                }
            }
        }
    }