using Confluent.Kafka;
using Kafka.Common;
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

        private readonly TimeSpan maxTaskIdle;
        private readonly int maxBufferedSize;
        private readonly PartitionGroup partitionGroup;
        private readonly IRecordCollector recordCollector;
        private readonly RecordInfo recordInfo;
        private readonly Dictionary<TopicPartition, long> consumedOffsets;
        private readonly PunctuationQueue streamTimePunctuationQueue;
        private readonly PunctuationQueue systemTimePunctuationQueue;
        private readonly IProducerSupplier producerSupplier;

        private DateTime idleStartTime;
        private IProducer<byte[], byte[]> producer;
        public bool commitRequested { get; private set; } = false;
        private bool transactionInFlight = false;

        public StreamTask(
            KafkaStreamsContext context,
            TaskId id,
            List<TopicPartition> partitions,
            ProcessorTopology topology,
            IConsumer<byte[], byte[]> consumer,
            IChangelogReader changelogReader,
            StreamsConfig config,
            StateDirectory stateDirectory,
            ThreadCache cache,
            IProducerSupplier producerSupplier)
            : this(
                  context,
                  id,
                  partitions,
                  topology,
                  consumer,
                  changelogReader,
                  config,
                  stateDirectory,
                  cache,
                  producerSupplier,
                  null)
        {
        }

        public StreamTask(
            KafkaStreamsContext context,
            TaskId id,
            List<TopicPartition> partitions,
            ProcessorTopology topology,
            IConsumer<byte[], byte[]> consumer,
            IChangelogReader changelogReader,
            StreamsConfig config,
            StateDirectory stateDirectory,
            ThreadCache cache,
            IProducerSupplier producerSupplier,
            IRecordCollector recordCollector)
            : base(
                  context,
                  id,
                  partitions,
                  topology,
                  consumer,
                  changelogReader,
                  isStandby: false,
                  stateDirectory,
                  config)
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

            this.producerSupplier = producerSupplier ?? throw new ArgumentNullException(nameof(producerSupplier));
            this.producer = producerSupplier.Get();

            IProductionExceptionHandler productionExceptionHandler = config.DefaultProductionExceptionHandler(this.Context);

            if (recordCollector == null)
            {
                this.recordCollector = new RecordCollector(
                    id.ToString(),
                    productionExceptionHandler);
            }
            else
            {
                this.recordCollector = recordCollector;
            }

            this.recordCollector.Init(this.producer);

            this.streamTimePunctuationQueue = new PunctuationQueue();
            this.systemTimePunctuationQueue = new PunctuationQueue();
            this.maxTaskIdle = TimeSpan.FromMilliseconds(config.GetLong(StreamsConfig.MAX_TASK_IDLE_MS_CONFIGConfig) ?? 0L);
            this.maxBufferedSize = config.GetInt(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIGConfig) ?? 1000;

            // initialize the consumed and committed offset cache
            this.consumedOffsets = new Dictionary<TopicPartition, long>();

            // create queues for each assigned partition and associate them
            // to corresponding source nodes in the processor topology
            var partitionQueues = new Dictionary<TopicPartition, RecordQueue>();

            // initialize the topology with its own context
            var processorContextImpl = new ProcessorContext<byte[], byte[]>(
                this.Context,
                id,
                this,
                config,
                this.recordCollector,
                this.StateMgr,
                cache);

            this.processorContext = processorContextImpl;

            ITimestampExtractor defaultTimestampExtractor = config.GetDefaultTimestampExtractor(this.Context);
            IDeserializationExceptionHandler defaultDeserializationExceptionHandler = config.GetDefaultDeserializationExceptionHandler(this.Context);

            foreach (TopicPartition partition in partitions)
            {
                var source = topology.Source(partition.Topic);

                ITimestampExtractor sourceTimestampExtractor = source.TimestampExtractor ?? defaultTimestampExtractor;

                var queue = new RecordQueue<byte[], byte[]>(
                    partition,
                    source,
                    sourceTimestampExtractor,
                    defaultDeserializationExceptionHandler,
                    this.processorContext);

                partitionQueues.Add(partition, queue);
            }

            this.recordInfo = new RecordInfo();
            // partitionGroup = new PartitionGroup(partitionQueues, processorContextImpl);

            this.StateMgr.RegisterGlobalStateStores(topology.globalStateStores);

            // initialize transactions if eos is turned on, which will block if the previous transaction has not
            // completed yet; do not start the first transaction until the topology has been initialized later
            if (this.eosEnabled)
            {
                this.InitializeTransactions();
            }
        }

        public override bool InitializeStateStores()
        {
            this.logger.LogTrace("Initializing state stores");
            this.RegisterStateStores();

            return !this.changelogPartitions.Any();
        }

        /**
         * <pre>
         * - (re-)initialize the topology of the task
         * </pre>
         *
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public override void InitializeTopology()
        {
            this.InitTopology();

            if (this.IsEosEnabled())
            {
                try
                {
                    //this.producer.beginTransaction();
                }
                catch (ProducerFencedException fatal)
                {
                    throw new KafkaException(new Confluent.Kafka.Error(ErrorCode.TransactionCoordinatorFenced, "", true), fatal);
                }

                this.transactionInFlight = true;
            }

            this.processorContext.Initialize();

            this.TaskInitialized = true;

            this.idleStartTime = RecordQueue.UNKNOWN;

            this.StateMgr.EnsureStoresRegistered();
        }

        /**
         * <pre>
         * - resume the task
         * </pre>
         */
        public override void Resume()
        {
            this.logger.LogDebug("Resuming");
            if (this.eosEnabled)
            {
                if (this.producer != null)
                {
                    throw new InvalidOperationException("Task producer should be null.");
                }

                this.producer = this.producerSupplier.Get();
                this.InitializeTransactions();
                this.recordCollector.Init(this.producer);

                try
                {

                    this.StateMgr.ClearCheckpoints();
                }
                catch (IOException e)
                {
                    throw new ProcessorStateException(string.Format("%sError while deleting the checkpoint file", this.logPrefix), e);
                }
            }
        }

        /**
         * An active task is processable if its buffer contains data for All of its input
         * source topic partitions, or if it is enforced to be processable
         */
        public bool IsProcessable(DateTime now)
        {
            if (this.partitionGroup.AllPartitionsBuffered())
            {
                this.idleStartTime = RecordQueue.UNKNOWN;
                return true;
            }
            else if (this.partitionGroup.NumBuffered() > 0)
            {
                if (this.idleStartTime == RecordQueue.UNKNOWN)
                {
                    this.idleStartTime = now;
                }

                if (now - this.idleStartTime >= this.maxTaskIdle)
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
            var record = this.partitionGroup.NextRecord<byte[], byte[]>(this.recordInfo);

            // if there is no record to process, return immediately
            if (record == null)
            {
                return false;
            }

            try
            {
                // process the record by passing to the source node of the topology
                var currNode = (ProcessorNode<byte[], byte[]>)this.recordInfo.Node();
                TopicPartition partition = this.recordInfo.Partition();

                this.logger.LogTrace("Start processing one record [{}]", record);

                //updateProcessorContext(record, currNode);
                //((ProcessorNode<byte[], byte[]>)currNode).process(record.Key, record.Value);

                this.logger.LogTrace("Completed processing one record [{}]", record);

                // update the consumed offset map after processing is done
                this.consumedOffsets.Add(partition, record.offset);
                this.commitNeeded = true;

                // after processing this record, if its partition queue's buffered size has been
                // decreased to the threshold, we can then resume the consumption on this partition
                if (this.recordInfo.queue.Size() == this.maxBufferedSize)
                {
                    this.consumer.Resume(new[] { partition });
                }
            }
            catch (ProducerFencedException fatal)
            {
                throw;// new TaskMigratedException(this, fatal);
            }
            catch (KafkaException e)
            {
                var stackTrace = this.GetStacktraceString(e);
                throw new StreamsException(string.Format("Exception caught in process. taskId=%s, " +
                        "processor=%s, topic=%s, partition=%d, offset=%d, stacktrace=%s",
                    this.id,
                    this.processorContext.GetCurrentNode().Name,
                    record.Topic,
                    record.partition,
                    record.offset,
                    stackTrace), e);
            }
            finally
            {
                this.processorContext.SetCurrentNode(null);
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
        public void Punctuate(IProcessorNode<byte[], byte[]> node, DateTime timestamp, PunctuationType type, IPunctuator punctuator)
        {
            if (this.processorContext.GetCurrentNode() != null)
            {
                throw new InvalidOperationException($"{this.logPrefix}Current node is not null");
            }

            this.UpdateProcessorContext(new StampedRecord(DUMMY_RECORD, timestamp), node);

            this.logger.LogTrace("Punctuating processor {} with timestamp {} and punctuation type {}", node.Name, timestamp, type);

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
                this.processorContext.SetCurrentNode(null);
            }
        }

        private void UpdateProcessorContext(StampedRecord record, IProcessorNode<byte[], byte[]> currNode)
        {
            this.processorContext.SetRecordContext(
                new ProcessorRecordContext(
                    record.timestamp,
                    record.offset,
                    record.partition,
                    record.Topic,
                    record.Headers));

            this.processorContext.SetCurrentNode(currNode);
        }

        /**
         * <pre>
         * - Flush state and producer
         * - if(!eos) write checkpoint
         * - commit offsets and start new transaction
         * </pre>
         *
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        public override void Commit()
        {
            this.Commit(true);
        }

        /**
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        // visible for testing
        private void Commit(bool startNewTransaction)
        {
            var startNs = this.Context.Clock.NowAsEpochNanoseconds;
            this.logger.LogDebug("Committing");

            this.FlushState();

            if (!this.eosEnabled)
            {
                this.StateMgr.Checkpoint(this.ActiveTaskCheckpointableOffsets());
            }

            var consumedOffsetsAndMetadata = new Dictionary<TopicPartition, OffsetAndMetadata>(this.consumedOffsets.Count);

            foreach (var entry in this.consumedOffsets)
            {
                TopicPartition partition = entry.Key;
                var offset = entry.Value + 1;
                consumedOffsetsAndMetadata.Add(partition, new OffsetAndMetadata(offset));
                this.StateMgr.PutOffsetLimit(partition, offset);
            }

            try
            {

                if (this.eosEnabled)
                {
                    //producer.sendOffsetsToTransaction(consumedOffsetsAndMetadata, applicationId);
                    //producer.commitTransaction();
                    this.transactionInFlight = false;
                    if (startNewTransaction)
                    {
                        //  producer.beginTransaction();
                        this.transactionInFlight = true;
                    }
                }
                else
                {
                    this.consumer.Commit(consumedOffsetsAndMetadata.Select(tpo => new TopicPartitionOffset(new TopicPartition(tpo.Key.Topic, tpo.Key.Partition), tpo.Value.offset)));
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

            this.commitNeeded = false;
            this.commitRequested = false;
            //taskMetrics.taskCommitTimeSensor.record(this.Context.Clock.NowAsEpochNanoseconds; - startNs);
        }

        public override Dictionary<TopicPartition, long> ActiveTaskCheckpointableOffsets()
        {
            var checkpointableOffsets = this.recordCollector.offsets;

            foreach (var entry in this.consumedOffsets)
            {
                checkpointableOffsets.TryAdd(entry.Key, entry.Value);
            }

            return checkpointableOffsets;
        }

        protected override void FlushState()
        {
            this.logger.LogTrace("Flushing state and producer");
            base.FlushState();

            try
            {
                this.recordCollector.Flush();
            }
            catch (ProducerFencedException fatal)
            {
                throw; // new TaskMigratedException(this, fatal);
            }
        }

        public Dictionary<TopicPartition, long> PurgableOffsets()
        {
            var purgableConsumedOffsets = new Dictionary<TopicPartition, long>();
            foreach (KeyValuePair<TopicPartition, long> entry in this.consumedOffsets)
            {
                TopicPartition tp = entry.Key;
                if (this.topology.IsRepartitionTopic(tp.Topic))
                {
                    purgableConsumedOffsets.Add(tp, entry.Value + 1);
                }
            }

            return purgableConsumedOffsets;
        }

        private void InitTopology()
        {
            // initialize the task by initializing All its processor nodes in the topology
            this.logger.LogTrace("Initializing processor nodes of the topology");
            foreach (ProcessorNode<byte[], byte[]> node in this.topology.Processors())
            {
                this.processorContext.SetCurrentNode(node);
                try
                {
                    node.Init(this.processorContext);
                }
                finally
                {
                    this.processorContext.SetCurrentNode(null);
                }
            }
        }

        /**
         * <pre>
         * - Close topology
         * - {@link #commit()}
         *   - Flush state and producer
         *   - if (!eos) write checkpoint
         *   - commit offsets
         * </pre>
         *
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        public override void Suspend()
        {
            this.logger.LogDebug("Suspending");
            this.Suspend(true, false);
        }

        /**
         * <pre>
         * - Close topology
         * - if (clean) {@link #commit()}
         *   - Flush state and producer
         *   - if (!eos) write checkpoint
         *   - commit offsets
         * </pre>
         *
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        // visible for testing
        private void Suspend(bool clean, bool isZombie)
        {
            try
            {
                this.CloseTopology(); // should we call this only on clean suspend?
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
                    this.Commit(false);
                }
                finally
                {
                    if (this.eosEnabled)
                    {
                        this.StateMgr.Checkpoint(this.ActiveTaskCheckpointableOffsets());

                        try
                        {
                            this.recordCollector.Close();
                        }
                        catch (ProducerFencedException e)
                        {
                            // taskMigratedException = new TaskMigratedException(this, e);
                        }
                        finally
                        {
                            this.producer = null;
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
                this.MaybeAbortTransactionAndCloseRecordCollector(isZombie);
            }
        }

        private void MaybeAbortTransactionAndCloseRecordCollector(bool isZombie)
        {
            if (this.eosEnabled && !isZombie)
            {
                try
                {
                    if (this.transactionInFlight)
                    {
                        //producer.abortTransaction();
                    }

                    this.transactionInFlight = false;
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

            if (this.eosEnabled)
            {
                try
                {
                    this.recordCollector.Close();
                }
                catch (Exception e)
                {
                    this.logger.LogError("Failed to Close producer due to the following error:", e);
                }
                finally
                {
                    this.producer = null;
                }
            }
        }

        private void CloseTopology()
        {
            this.logger.LogTrace("Closing processor topology");

            this.partitionGroup.Clear();

            // Close the processors
            // make sure Close() is called for each node even when there is a RuntimeException
            RuntimeException exception = null;
            if (this.TaskInitialized)
            {
                foreach (ProcessorNode<byte[], byte[]> node in this.topology.Processors())
                {
                    this.processorContext.SetCurrentNode(node);

                    try
                    {
                        node.Close();
                    }
                    catch (RuntimeException e)
                    {
                        exception = e;
                    }
                    finally
                    {

                        this.processorContext.SetCurrentNode(null);
                    }
                }
            }

            if (exception != null)
            {
                throw exception;
            }
        }

        // helper to avoid calling suspend() twice if a suspended task is not reassigned and closed

        public override void CloseSuspended(
            bool clean,
            bool isZombie,
            RuntimeException firstException)
        {
            try
            {
                this.CloseStateManager(clean);
            }
            catch (RuntimeException e)
            {
                if (firstException == null)
                {
                    firstException = e;
                }
                this.logger.LogError("Could not Close state manager due to the following error:", e);
            }

            this.partitionGroup.Close();
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
         *   - Close topology
         *   - if (clean) {@link #commit()}
         *     - Flush state and producer
         *     - commit offsets
         * - Close state
         *   - if (clean) write checkpoint
         * - if (eos) Close producer
         * </pre>
         *
         * @param clean    shut down cleanly (ie, incl. Flush and commit) if {@code true} --
         *                 otherwise, just Close open resources
         * @param isZombie {@code true} is this task is a zombie or not (this will repress {@link TaskMigratedException}
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */
        public override void Close(
            bool clean,
            bool isZombie)
        {
            this.logger.LogDebug("Closing");

            RuntimeException firstException = null;
            try
            {

                this.Suspend(clean, isZombie);
            }
            catch (RuntimeException e)
            {
                clean = false;
                firstException = e;
                this.logger.LogError("Could not Close task due to the following error:", e);
            }

            this.CloseSuspended(clean, isZombie, firstException);

            this.TaskClosed = true;
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
            var newQueueSize = this.partitionGroup.AddRawRecords(partition, records);
            this.logger.LogTrace("Added records into the buffered queue of partition {}, new queue size is {}", partition, newQueueSize);

            // if after.Adding these records, its partition queue's buffered size has been
            // increased beyond the threshold, we can then pause the consumption for this partition
            if (newQueueSize > this.maxBufferedSize)
            {
                this.consumer.Pause(new[] { partition });
            }
        }

        /**
         * Schedules a punctuation for the processor
         *
         * @param interval the interval in milliseconds
         * @param type     the punctuation type
         * @throws InvalidOperationException if the current node is not null
         */
        public ICancellable Schedule(TimeSpan interval, PunctuationType type, IPunctuator punctuator)
        {
            return type switch
            {
                PunctuationType.STREAM_TIME => this.Schedule(DateTime.MinValue, interval, type, punctuator),
                PunctuationType.WALL_CLOCK_TIME => this.Schedule(this.Context.Clock.UtcNow + interval, interval, type, punctuator),
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
        private ICancellable Schedule(DateTime startTime, TimeSpan interval, PunctuationType type, IPunctuator punctuator)
        {
            if (this.processorContext.GetCurrentNode() == null)
            {
                throw new InvalidOperationException($"{this.logPrefix}Current node is null");
            }

            var schedule = new PunctuationSchedule(this.processorContext.GetCurrentNode(), startTime, interval, punctuator);

            return type switch
            {
                PunctuationType.STREAM_TIME => this.streamTimePunctuationQueue.Schedule(schedule),
                PunctuationType.WALL_CLOCK_TIME => this.systemTimePunctuationQueue.Schedule(schedule),
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
            var streamTime = this.partitionGroup.streamTime;

            // if the timestamp is not known yet, meaning there is not enough data accumulated
            // to reason stream partition time, then skip.
            if (streamTime == RecordQueue.UNKNOWN)
            {
                return false;
            }
            else
            {

                var punctuated = this.streamTimePunctuationQueue.MayPunctuate(streamTime, PunctuationType.STREAM_TIME, this);

                if (punctuated)
                {
                    this.commitNeeded = true;
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
            var punctuated = this.systemTimePunctuationQueue.MayPunctuate(this.Context.Clock.UtcNow, PunctuationType.WALL_CLOCK_TIME, this);

            if (punctuated)
            {
                this.commitNeeded = true;
            }

            return punctuated;
        }

        /**
         * Request committing the current task's state
         */
        public void RequestCommit()
        {
            this.commitRequested = true;
        }

        /**
         * Whether or not a request has been made to commit the current state
         */
        public IProducer<byte[], byte[]> GetProducer()
        {
            return this.producer;
        }

        private void InitializeTransactions()
        {
            try
            {
                //producer.initTransactions();
            }
            catch (TimeoutException retriable)
            {
                this.logger.LogError(
                    "Timeout exception caught when initializing transactions for task {}. " +
                        "This might happen if the broker is slow to respond, if the network connection to " +
                        "the broker was interrupted, or if similar circumstances arise. " +
                        "You can increase producer parameter `max.block.ms` to increase this timeout.",
                    this.id,
                    retriable);

                throw new StreamsException(
                    $"{this.logPrefix}Failed to initialize task {this.id} due to timeout.",
                    retriable);
            }
        }

        public override void InitializeIfNeeded()
        {
            if (false)//state() == State.CREATED)
            {
                //recordCollector.initialize();

                //StateManagerUtil.registerStateStores(log, logPrefix, topology, stateMgr, stateDirectory, processorContext);

                //transitionTo(State.RESTORING);

                this.logger.LogInformation("Initialized");
            }
        }
        private void InitializeMetadata()
        {
            try
            {
                var offsetsAndMetadata = this.consumer.Committed(this.partitions, TimeSpan.FromSeconds(10.0))
                    .Where(e => e != null)
                    .ToDictionary(k => k.TopicPartition, v => v);

                this.InitializeTaskTime(offsetsAndMetadata);
            }
            catch (TimeoutException e)
            {
                this.logger.LogWarning("Encountered {} while trying to Fetch committed offsets, will retry initializing the metadata in the next loop." +
                    "\nConsider overwriting consumer config {} to a larger value to avoid timeout errors",
                    e.ToString(),
                    "default.api.timeout.ms");

                throw;
            }
            catch (KafkaException e)
            {
                throw new StreamsException($"task [{this.id}] Failed to initialize offsets for {this.partitions}", e);
            }
        }

        public override void CompleteRestoration()
        {
            if (false)//state() == State.RESTORING)
            {
                this.InitializeMetadata();
                this.InitializeTopology();
                this.processorContext.Initialize();
                this.idleStartTime = RecordQueue.UNKNOWN;
                //transitionTo(State.RUNNING);

                this.logger.LogInformation("Restored and ready to run");
            }
            else
            {
                throw new Exception("Illegal state " + "TESTstate()" + " while completing restoration for active task " + this.id);
            }
        }

        private DateTime DecodeTimestamp(string encryptedString)
        {
            if (!encryptedString.Any())
            {
                return RecordQueue.UNKNOWN;
            }

            ByteBuffer buffer = null;// new ByteBuffer().Wrap(Base64.DecodeFromUtf8InPlace(encryptedString));
            byte? version = null;// buffer.GetLong;
            switch (version)
            {
                //case LATEST_MAGIC_BYTE:
                //    return buffer.GetLong();
                default:
                    //log.LogWarning("Unsupported offset metadata version found. Supported version {}. Found version {}.",
                    //         LATEST_MAGIC_BYTE, version);

                    return RecordQueue.UNKNOWN;
            }
        }

        private void InitializeTaskTime(Dictionary<TopicPartition, TopicPartitionOffset> offsetsAndMetadata)
        {
            foreach (var entry in offsetsAndMetadata)
            {
                TopicPartition partition = entry.Key;
                var metadata = entry.Value;

                if (metadata != null)
                {
                    //long committedTimestamp = DecodeTimestamp(metadata);
                    //partitionGroup.SetPartitionTime(partition, committedTimestamp);
                    this.logger.LogDebug($"A committed timestamp was detected: setting the partition time of partition {partition}");
                    //+ $" to {committedTimestamp} in stream task {id}");
                }
                else
                {
                    this.logger.LogDebug("No committed timestamp was found in metadata for partition {}", partition);
                }
            }

            var nonCommitted = new HashSet<TopicPartition>(this.partitions);
            nonCommitted.RemoveWhere(tp => offsetsAndMetadata.Keys.Contains(tp));

            foreach (var partition in nonCommitted)
            {
                this.logger.LogDebug("No committed offset for partition {}, therefore no timestamp can be found for this partition", partition);
            }
        }
    }
}
