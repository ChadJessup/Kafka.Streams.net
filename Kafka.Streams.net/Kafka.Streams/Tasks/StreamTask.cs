using Confluent.Kafka;
using Kafka.Streams.Clients;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Errors.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Microsoft.Extensions.Logging;
using NodaTime;
using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Kafka.Streams.Tasks
{
    /**
     * A StreamTask is associated with a {@link PartitionGroup}, and is assigned to a KafkaStreamThread for processing.
     */
    public class StreamTask : AbstractTask, IProcessorNodePunctuator<byte[], byte[]>
    {
        private static readonly ConsumeResult<object, object> DUMMY_RECORD = new ConsumeResult<object, object>();// ProcessorContextImpl.NONEXIST_TOPIC, -1, -1L, null, null);

        private readonly IClock clock;
        private readonly long maxTaskIdleMs;
        private readonly int maxBufferedSize;
        private readonly PartitionGroup partitionGroup;
        private readonly IRecordCollector recordCollector;
        private readonly RecordInfo recordInfo;
        private readonly Dictionary<TopicPartition, long> consumedOffsets;
        private readonly PunctuationQueue streamTimePunctuationQueue;
        private readonly PunctuationQueue systemTimePunctuationQueue;
        private readonly IProducerSupplier producerSupplier;

        private long idleStartTime;
        private IProducer<byte[], byte[]> producer;
        public bool commitRequested { get; private set; } = false;
        private bool transactionInFlight = false;

        public StreamTask(
            TaskId id,
            List<TopicPartition> partitions,
            ProcessorTopology topology,
            IConsumer<byte[], byte[]> consumer,
            IChangelogReader changelogReader,
            StreamsConfig config,
            StateDirectory stateDirectory,
            ThreadCache cache,
            IClock clock,
            IProducerSupplier producerSupplier)
            : this(id, partitions, topology, consumer, changelogReader, config, stateDirectory, cache, clock, producerSupplier, null)
        {
        }

        public StreamTask(
            TaskId id,
            List<TopicPartition> partitions,
            ProcessorTopology topology,
            IConsumer<byte[], byte[]> consumer,
            IChangelogReader changelogReader,
            StreamsConfig config,
            StateDirectory stateDirectory,
            ThreadCache cache,
            IClock clock,
            IProducerSupplier producerSupplier,
            IRecordCollector recordCollector)
            : base(id, partitions, topology, consumer, changelogReader, false, stateDirectory, config)
        {
            if (id is null)
            {
                throw new ArgumentNullException(nameof(id));
            }

            if (partitions is null)
            {
                throw new ArgumentNullException(nameof(partitions));
            }

            if (topology is null)
            {
                throw new ArgumentNullException(nameof(topology));
            }

            if (consumer is null)
            {
                throw new ArgumentNullException(nameof(consumer));
            }

            if (changelogReader is null)
            {
                throw new ArgumentNullException(nameof(changelogReader));
            }

            if (config is null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            if (stateDirectory is null)
            {
                throw new ArgumentNullException(nameof(stateDirectory));
            }

            if (cache is null)
            {
                throw new ArgumentNullException(nameof(cache));
            }

            this.clock = clock ?? throw new ArgumentNullException(nameof(clock));
            this.producerSupplier = producerSupplier ?? throw new ArgumentNullException(nameof(producerSupplier));
            this.producer = producerSupplier.Get();

            IProductionExceptionHandler productionExceptionHandler = config.DefaultProductionExceptionHandler();

            if (recordCollector == null)
            {
                this.recordCollector = new RecordCollectorImpl(
                    id.ToString(),
                    productionExceptionHandler);
            }
            else
            {
                this.recordCollector = recordCollector;
            }

            this.recordCollector.init(this.producer);

            streamTimePunctuationQueue = new PunctuationQueue();
            systemTimePunctuationQueue = new PunctuationQueue();
            maxTaskIdleMs = config.getLong(StreamsConfigPropertyNames.MAX_TASK_IDLE_MS_CONFIG).Value;
            maxBufferedSize = config.GetInt(StreamsConfigPropertyNames.BUFFERED_RECORDS_PER_PARTITION_CONFIG).Value;

            // initialize the consumed and committed offset cache
            consumedOffsets = new Dictionary<TopicPartition, long>();

            // create queues for each assigned partition and associate them
            // to corresponding source nodes in the processor topology
            var partitionQueues = new Dictionary<TopicPartition, RecordQueue>();

            // initialize the topology with its own context
            var processorContextImpl = new ProcessorContext<byte[], byte[]>(
                id,
                this,
                config,
                this.recordCollector,
                StateMgr,
                cache);

            processorContext = processorContextImpl;

            ITimestampExtractor defaultTimestampExtractor = config.DefaultTimestampExtractor();
            IDeserializationExceptionHandler defaultDeserializationExceptionHandler = config.DefaultDeserializationExceptionHandler();

            foreach (TopicPartition partition in partitions)
            {
                var source = topology.Source(partition.Topic);

                ITimestampExtractor sourceTimestampExtractor = source.TimestampExtractor ?? defaultTimestampExtractor;

                var queue = new RecordQueue<byte[], byte[]>(
                    partition,
                    (ISourceNode<byte[], byte[]>)source,
                    sourceTimestampExtractor,
                    defaultDeserializationExceptionHandler,
                    processorContext);

                partitionQueues.Add(partition, queue);
            }

            recordInfo = new RecordInfo();
            // partitionGroup = new PartitionGroup(partitionQueues, processorContextImpl);

            StateMgr.registerGlobalStateStores(topology.globalStateStores);

            // initialize transactions if eos is turned on, which will block if the previous transaction has not
            // completed yet; do not start the first transaction until the topology has been initialized later
            if (eosEnabled)
            {
                initializeTransactions();
            }
        }

        public override bool initializeStateStores()
        {
            logger.LogTrace("Initializing state stores");
            registerStateStores();

            return !changelogPartitions.Any();
        }

        /**
         * <pre>
         * - (re-)initialize the topology of the task
         * </pre>
         *
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public override void initializeTopology()
        {
            InitTopology();

            if (isEosEnabled())
            {
                try
                {
                    //this.producer.beginTransaction();
                }
                catch (ProducerFencedException fatal)
                {
                    throw new KafkaException(new Error(ErrorCode.TransactionCoordinatorFenced, "", true), fatal);
                }

                transactionInFlight = true;
            }

            processorContext.initialize();

            TaskInitialized = true;

            idleStartTime = RecordQueue.UNKNOWN;

            StateMgr.ensureStoresRegistered();
        }

        /**
         * <pre>
         * - resume the task
         * </pre>
         */
        public override void resume()
        {
            logger.LogDebug("Resuming");
            if (eosEnabled)
            {
                if (producer != null)
                {
                    throw new InvalidOperationException("Task producer should be null.");
                }

                producer = producerSupplier.Get();
                initializeTransactions();
                recordCollector.init(producer);

                try
                {

                    StateMgr.clearCheckpoints();
                }
                catch (IOException e)
                {
                    throw new ProcessorStateException(string.Format("%sError while deleting the checkpoint file", logPrefix), e);
                }
            }
        }

        /**
         * An active task is processable if its buffer contains data for all of its input
         * source topic partitions, or if it is enforced to be processable
         */
        public bool IsProcessable(long now)
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
                    //taskMetrics.taskEnforcedProcessSensor.record();
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
        public bool Process()
        {
            // get the next record to process
            var record = partitionGroup.nextRecord<byte[], byte[]>(recordInfo);

            // if there is no record to process, return immediately
            if (record == null)
            {
                return false;
            }

            try
            {
                // process the record by passing to the source node of the topology
                var currNode = (ProcessorNode<byte[], byte[]>)recordInfo.node();
                TopicPartition partition = recordInfo.partition();

                logger.LogTrace("Start processing one record [{}]", record);

                //updateProcessorContext(record, currNode);
                //((ProcessorNode<byte[], byte[]>)currNode).process(record.Key, record.Value);

                logger.LogTrace("Completed processing one record [{}]", record);

                // update the consumed offset map after processing is done
                consumedOffsets.Add(partition, record.offset);
                commitNeeded = true;

                // after processing this record, if its partition queue's buffered size has been
                // decreased to the threshold, we can then resume the consumption on this partition
                if (recordInfo.queue.size() == maxBufferedSize)
                {
                    consumer.Resume(new[] { partition });
                }
            }
            catch (ProducerFencedException fatal)
            {
                throw;// new TaskMigratedException(this, fatal);
            }
            catch (KafkaException e)
            {
                var stackTrace = GetStacktraceString(e);
                throw new StreamsException(string.Format("Exception caught in process. taskId=%s, " +
                        "processor=%s, topic=%s, partition=%d, offset=%d, stacktrace=%s",
                    id,
                    processorContext.GetCurrentNode().Name,
                    record.Topic,
                    record.partition,
                    record.offset,
                    stackTrace), e);
            }
            finally
            {
                processorContext.SetCurrentNode(null);
            }

            return true;
        }

        private string GetStacktraceString(KafkaException e)
        {
            var stacktrace = e.StackTrace;
            using var stringWriter = new StringWriter();
            var printWriter = new PrintWriter(stringWriter);
            try
            {
                //e.printStackTrace(printWriter);
                stacktrace = stringWriter.ToString();
            }
            catch (IOException ioe)
            {
                //log.LogError("Encountered error extracting stacktrace from this exception", ioe);
            }

            return stacktrace;
        }

        /**
         * @throws InvalidOperationException if the current node is not null
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public void punctuate(IProcessorNode<byte[], byte[]> node, long timestamp, PunctuationType type, IPunctuator punctuator)
        {
            if (processorContext.GetCurrentNode() != null)
            {
                throw new InvalidOperationException(string.Format("%sCurrent node is not null", logPrefix));
            }

            UpdateProcessorContext(new StampedRecord(DUMMY_RECORD, timestamp), node);

            logger.LogTrace("Punctuating processor {} with timestamp {} and punctuation type {}", node.Name, timestamp, type);

            try
            {
                node.Punctuate(timestamp, punctuator);
            }
            catch (ProducerFencedException fatal)
            {
                throw;// new TaskMigratedException(this, fatal);
            }
            catch (KafkaException e)
            {
                throw new StreamsException($"Exception caught while punctuating processor", e);
            }
            finally
            {
                processorContext.SetCurrentNode(null);
            }
        }

        private void UpdateProcessorContext(StampedRecord record, IProcessorNode<byte[], byte[]> currNode)
        {
            processorContext.setRecordContext(
                new ProcessorRecordContext(
                    record.timestamp,
                    record.offset,
                    record.partition,
                    record.Topic,
                    record.Headers));

            processorContext.SetCurrentNode(currNode);
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
        public override void commit()
        {
            Commit(true);
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        // visible for testing
        void Commit(bool startNewTransaction)
        {
            var startNs = clock.GetCurrentInstant().ToUnixTimeTicks() * NodaConstants.NanosecondsPerTick;
            logger.LogDebug("Committing");

            flushState();

            if (!eosEnabled)
            {
                StateMgr.checkpoint(activeTaskCheckpointableOffsets());
            }

            var consumedOffsetsAndMetadata = new Dictionary<TopicPartition, OffsetAndMetadata>(consumedOffsets.Count);

            foreach (var entry in consumedOffsets)
            {
                TopicPartition partition = entry.Key;
                var offset = entry.Value + 1;
                consumedOffsetsAndMetadata.Add(partition, new OffsetAndMetadata(offset));
                StateMgr.putOffsetLimit(partition, offset);
            }

            try
            {

                if (eosEnabled)
                {
                    //producer.sendOffsetsToTransaction(consumedOffsetsAndMetadata, applicationId);
                    //producer.commitTransaction();
                    transactionInFlight = false;
                    if (startNewTransaction)
                    {
                        //  producer.beginTransaction();
                        transactionInFlight = true;
                    }
                }
                else
                {
                    consumer.Commit(consumedOffsetsAndMetadata.Select(tpo => new TopicPartitionOffset(new TopicPartition(tpo.Key.Topic, tpo.Key.Partition), tpo.Value.offset)));
                }
            }
            catch (CommitFailedException cfe)
            {
                throw; // new TaskMigratedException(this, error);
            }
            catch (ProducerFencedException error)
            {
                throw; // new TaskMigratedException(this, error);
            }

            commitNeeded = false;
            commitRequested = false;
            //taskMetrics.taskCommitTimeSensor.record(time.nanoseconds() - startNs);
        }

        public override Dictionary<TopicPartition, long> activeTaskCheckpointableOffsets()
        {
            var checkpointableOffsets = recordCollector.offsets;

            foreach (var entry in consumedOffsets)
            {
                checkpointableOffsets.TryAdd(entry.Key, entry.Value);
            }

            return checkpointableOffsets;
        }

        protected override void flushState()
        {
            logger.LogTrace("Flushing state and producer");
            base.flushState();

            try
            {
                recordCollector.flush();
            }
            catch (ProducerFencedException fatal)
            {
                throw; // new TaskMigratedException(this, fatal);
            }
        }

        public Dictionary<TopicPartition, long> PurgableOffsets()
        {
            var purgableConsumedOffsets = new Dictionary<TopicPartition, long>();
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

        private void InitTopology()
        {
            // initialize the task by initializing all its processor nodes in the topology
            logger.LogTrace("Initializing processor nodes of the topology");
            foreach (ProcessorNode<byte[], byte[]> node in topology.processors())
            {
                processorContext.SetCurrentNode(node);
                try
                {
                    node.Init(processorContext);
                }
                finally
                {
                    processorContext.SetCurrentNode(null);
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
        public override void suspend()
        {
            logger.LogDebug("Suspending");
            Suspend(true, false);
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
        void Suspend(bool clean, bool isZombie)
        {
            try
            {
                CloseTopology(); // should we call this only on clean suspend?
            }
            catch (RuntimeException fatal)
            {
                if (clean)
                {
                    throw;
                }
            }

            if (clean)
            {
                TaskMigratedException? taskMigratedException = null;

                try
                {
                    Commit(false);
                }
                finally
                {
                    if (eosEnabled)
                    {
                        StateMgr.checkpoint(activeTaskCheckpointableOffsets());

                        try
                        {
                            recordCollector.close();
                        }
                        catch (ProducerFencedException e)
                        {
                            // taskMigratedException = new TaskMigratedException(this, e);
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
                MaybeAbortTransactionAndCloseRecordCollector(isZombie);
            }
        }

        private void MaybeAbortTransactionAndCloseRecordCollector(bool isZombie)
        {
            if (eosEnabled && !isZombie)
            {
                try
                {
                    if (transactionInFlight)
                    {
                        //producer.abortTransaction();
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
                catch (Exception e)
                {
                    logger.LogError("Failed to close producer due to the following error:", e);
                }
                finally
                {
                    producer = null;
                }
            }
        }

        private void CloseTopology()
        {
            logger.LogTrace("Closing processor topology");

            partitionGroup.clear();

            // close the processors
            // make sure close() is called for each node even when there is a RuntimeException
            RuntimeException exception = null;
            if (TaskInitialized)
            {
                foreach (ProcessorNode<byte[], byte[]> node in topology.processors())
                {
                    processorContext.SetCurrentNode(node);

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

                        processorContext.SetCurrentNode(null);
                    }
                }
            }

            if (exception != null)
            {
                throw exception;
            }
        }

        // helper to avoid calling suspend() twice if a suspended task is not reassigned and closed

        public override void closeSuspended(
            bool clean,
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
                logger.LogError("Could not close state manager due to the following error:", e);
            }

            partitionGroup.close();
            //taskMetrics.removeAllSensors();

            //closeTaskSensor.record();

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
        public override void close(
            bool clean,
            bool isZombie)
        {
            logger.LogDebug("Closing");

            RuntimeException firstException = null;
            try
            {

                Suspend(clean, isZombie);
            }
            catch (RuntimeException e)
            {
                clean = false;
                firstException = e;
                logger.LogError("Could not close task due to the following error:", e);
            }

            closeSuspended(clean, isZombie, firstException);

            TaskClosed = true;
        }

        /**
         * Adds records to queues. If a record has an invalid (i.e., negative) timestamp, the record is skipped
         * and not.Added to the queue for processing
         *
         * @param partition the partition
         * @param records   the records
         */
        public void AddRecords(TopicPartition partition, IEnumerable<ConsumeResult<byte[], byte[]>> records)
        {
            var newQueueSize = partitionGroup.addRawRecords(partition, records);
            logger.LogTrace("Added records into the buffered queue of partition {}, new queue size is {}", partition, newQueueSize);

            // if after.Adding these records, its partition queue's buffered size has been
            // increased beyond the threshold, we can then pause the consumption for this partition
            if (newQueueSize > maxBufferedSize)
            {
                consumer.Pause(new[] { partition });
            }
        }

        /**
         * Schedules a punctuation for the processor
         *
         * @param interval the interval in milliseconds
         * @param type     the punctuation type
         * @throws InvalidOperationException if the current node is not null
         */
        public ICancellable Schedule(long interval, PunctuationType type, IPunctuator punctuator)
        {
            return type switch
            {
                PunctuationType.STREAM_TIME => Schedule(0L, interval, type, punctuator),
                PunctuationType.WALL_CLOCK_TIME => Schedule(clock.GetCurrentInstant().ToUnixTimeMilliseconds() + interval, interval, type, punctuator),
                _ => throw new ArgumentException("Unrecognized PunctuationType: " + type),
            };
        }

        /**
         * Schedules a punctuation for the processor
         *
         * @param startTime time of the first punctuation
         * @param interval  the interval in milliseconds
         * @param type      the punctuation type
         * @throws InvalidOperationException if the current node is not null
         */
        ICancellable Schedule(long startTime, long interval, PunctuationType type, IPunctuator punctuator)
        {
            if (processorContext.GetCurrentNode() == null)
            {
                throw new InvalidOperationException(string.Format("%sCurrent node is null", logPrefix));
            }

            var schedule = new PunctuationSchedule(processorContext.GetCurrentNode(), startTime, interval, punctuator);

            return type switch
            {
                PunctuationType.STREAM_TIME => streamTimePunctuationQueue.schedule(schedule),
                PunctuationType.WALL_CLOCK_TIME => systemTimePunctuationQueue.schedule(schedule),
                _ => throw new System.ArgumentException("Unrecognized PunctuationType: " + type),
            };
        }

        /**
         * Possibly trigger registered stream-time punctuation functions if
         * current partition group timestamp has reached the defined stamp
         * Note, this is only called in the presence of new records
         *
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public bool MaybePunctuateStreamTime()
        {
            var streamTime = partitionGroup.streamTime;

            // if the timestamp is not known yet, meaning there is not enough data accumulated
            // to reason stream partition time, then skip.
            if (streamTime == RecordQueue.UNKNOWN)
            {
                return false;
            }
            else
            {

                var punctuated = streamTimePunctuationQueue.mayPunctuate(streamTime, PunctuationType.STREAM_TIME, this);

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
        public bool MaybePunctuateSystemTime()
        {
            var systemTime = clock.GetCurrentInstant().ToUnixTimeMilliseconds();

            var punctuated = systemTimePunctuationQueue.mayPunctuate(systemTime, PunctuationType.WALL_CLOCK_TIME, this);

            if (punctuated)
            {
                commitNeeded = true;
            }

            return punctuated;
        }

        /**
         * Request committing the current task's state
         */
        public void RequestCommit()
        {
            commitRequested = true;
        }

        /**
         * Whether or not a request has been made to commit the current state
         */
        public IProducer<byte[], byte[]> getProducer()
        {
            return producer;
        }

        private void initializeTransactions()
        {
            try
            {
                //producer.initTransactions();
            }
            catch (TimeoutException retriable)
            {
                logger.LogError(
                    "Timeout exception caught when initializing transactions for task {}. " +
                        "This might happen if the broker is slow to respond, if the network connection to " +
                        "the broker was interrupted, or if similar circumstances arise. " +
                        "You can increase producer parameter `max.block.ms` to increase this timeout.",
                    id,
                    retriable
                );
                throw new StreamsException(
                    $"{logPrefix}Failed to initialize task {id} due to timeout.",
                    retriable
                );
            }
        }

        public override void initializeIfNeeded()
        {
            if (false)//state() == State.CREATED)
            {
                //recordCollector.initialize();

                //StateManagerUtil.registerStateStores(log, logPrefix, topology, stateMgr, stateDirectory, processorContext);

                //transitionTo(State.RESTORING);

                logger.LogInformation("Initialized");
            }
        }
        private void initializeMetadata()
        {
            try
            {
                var offsetsAndMetadata = this.consumer.Committed(partitions, TimeSpan.FromSeconds(10.0))
                    .Where(e => e != null)
                    .ToDictionary(k => k.TopicPartition, v => v);

                initializeTaskTime(offsetsAndMetadata);
            }
            catch (TimeoutException e)
            {
                logger.LogWarning("Encountered {} while trying to fetch committed offsets, will retry initializing the metadata in the next loop." +
                    "\nConsider overwriting consumer config {} to a larger value to avoid timeout errors",
                    e.ToString(),
                    "default.api.timeout.ms");

                throw;
            }
            catch (KafkaException e)
            {
                throw new StreamsException($"task [{id}] Failed to initialize offsets for {partitions}", e);
            }
        }

        public override void CompleteRestoration()
        {
            if (false)//state() == State.RESTORING)
            {
                initializeMetadata();
                initializeTopology();
                processorContext.initialize();
                idleStartTime = RecordQueue.UNKNOWN;
                //transitionTo(State.RUNNING);

                logger.LogInformation("Restored and ready to run");
            }
            else
            {
                throw new Exception("Illegal state " + "TESTstate()" + " while completing restoration for active task " + id);
            }
        }

        private long DecodeTimestamp(string encryptedString)
        {
            if (!encryptedString.Any())
            {
                return RecordQueue.UNKNOWN;
            }

            ByteBuffer buffer = null;// ByteBuffer.wrap(Base64.DecodeFromUtf8InPlace(encryptedString));
            byte? version = null;// buffer.getLong;
            switch (version)
            {
                //case LATEST_MAGIC_BYTE:
                //    return buffer.getLong();
                default:
                    //log.LogWarning("Unsupported offset metadata version found. Supported version {}. Found version {}.",
                    //         LATEST_MAGIC_BYTE, version);

                    return RecordQueue.UNKNOWN;
            }
        }

        private void initializeTaskTime(Dictionary<TopicPartition, TopicPartitionOffset> offsetsAndMetadata)
        {
            foreach (var entry in offsetsAndMetadata)
            {
                TopicPartition partition = entry.Key;
                var metadata = entry.Value;

                if (metadata != null)
                {
                    //long committedTimestamp = DecodeTimestamp(metadata);
                    //partitionGroup.SetPartitionTime(partition, committedTimestamp);
                    logger.LogDebug($"A committed timestamp was detected: setting the partition time of partition {partition}");
                    //+ $" to {committedTimestamp} in stream task {id}");
                }
                else
                {
                    logger.LogDebug("No committed timestamp was found in metadata for partition {}", partition);
                }
            }

            var nonCommitted = new HashSet<TopicPartition>(partitions);
            nonCommitted.RemoveWhere(tp => offsetsAndMetadata.Keys.Contains(tp));

            foreach (var partition in nonCommitted)
            {
                logger.LogDebug("No committed offset for partition {}, therefore no timestamp can be found for this partition", partition);
            }
        }
    }
}
