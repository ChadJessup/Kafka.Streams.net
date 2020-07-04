using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Kafka.Common;
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
using Kafka.Streams.Threads;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Tasks
{
    /**
     * A StreamTask is associated with a {@link PartitionGroup}, and is assigned to a StreamThread for processing.
     */
    public class StreamTask : AbstractTask, IProcessorNodePunctuator<object, object>
    {
        private static readonly ConsumeResult<object, object> DUMMY_RECORD = null; // new ConsumeResult<object, object>(ProcessorContext.NONEXIST_TOPIC, -1, -1L, null, null);
                                                                                   // visible for testing

        private const byte LATEST_MAGIC_BYTE = 1;

        private readonly ILogger<StreamTask> log;
        private readonly string logPrefix;
        private readonly IConsumer<byte[], byte[]> mainConsumer;

        // we want to abstract eos logic out of StreamTask, however
        // there's still an optimization that requires this info to be
        // leaked into this class, which is to checkpoint after committing if EOS is not enabled.
        private readonly bool eosEnabled;

        private readonly TimeSpan maxTaskIdle;
        private readonly int maxBufferedSize;
        private readonly PartitionGroup partitionGroup;
        private readonly RecordCollector recordCollector;
        private readonly RecordInfo recordInfo;
        private readonly Dictionary<TopicPartition, long> consumedOffsets;
        private readonly PunctuationQueue streamTimePunctuationQueue;
        private readonly PunctuationQueue systemTimePunctuationQueue;

        private TimeSpan processTime = TimeSpan.Zero;

        private readonly IInternalProcessorContext processorContext;

        private DateTime idleStartTime;
        private bool commitNeeded = false;
        private bool commitRequested = false;

        public override long LatestOffset { get; set; }
        public override TaskState CurrentState { get; }

        public StreamTask(
            KafkaStreamsContext context,
            TaskId id,
            HashSet<TopicPartition> partitions,
            ProcessorTopology topology,
            IConsumer<byte[], byte[]> mainConsumer,
            StreamsConfig config,
            StateDirectory stateDirectory,
            ThreadCache cache,
            ProcessorStateManager stateMgr,
            RecordCollector recordCollector)
            : base(
                  context,
                  id,
                  topology,
                  stateDirectory,
                  stateMgr,
                  partitions)
        {
            this.mainConsumer = mainConsumer;

            string threadIdPrefix = $"stream-thread [{Thread.CurrentThread.Name}] ";
            this.logPrefix = threadIdPrefix + $"{"task"} [{id}] ";
            //LogContext logContext = new LogContext(logPrefix);
            //log = logContext.logger(getClass());

            this.recordCollector = recordCollector;
            this.eosEnabled = false; // StreamThread.eosEnabled(config);

            string threadId = Thread.CurrentThread.Name;
            //closeTaskSensor = ThreadMetrics.closeTaskSensor(threadId, streamsMetrics);
            string taskId = id.ToString();
            // if (streamsMetrics.version() == Version.FROM_0100_TO_24)
            // {
            //     Sensor parent = ThreadMetrics.commitOverTasksSensor(threadId, streamsMetrics);
            //     enforcedProcessingSensor = TaskMetrics.enforcedProcessingSensor(threadId, taskId, streamsMetrics, parent);
            // }
            // else
            // {
            //     enforcedProcessingSensor = TaskMetrics.enforcedProcessingSensor(threadId, taskId, streamsMetrics);
            // }
            // processRatioSensor = TaskMetrics.activeProcessRatioSensor(threadId, taskId, streamsMetrics);
            // processLatencySensor = TaskMetrics.processLatencySensor(threadId, taskId, streamsMetrics);
            // punctuateLatencySensor = TaskMetrics.punctuateSensor(threadId, taskId, streamsMetrics);
            // bufferedRecordsSensor = TaskMetrics.activeBufferedRecordsSensor(threadId, taskId, streamsMetrics);

            this.streamTimePunctuationQueue = new PunctuationQueue();
            this.systemTimePunctuationQueue = new PunctuationQueue();
            this.maxTaskIdle = config.MaxTaskIdleDuration;
            this.maxBufferedSize = config.GetInt(StreamsConfig.BufferedRecordsPerPartitionConfig).Value;

            // initialize the consumed and committed offset cache
            this.consumedOffsets = new Dictionary<TopicPartition, long>();

            // create queues for each assigned partition and associate them
            // to corresponding source nodes in the processor topology
            Dictionary<TopicPartition, RecordQueue> partitionQueues = new Dictionary<TopicPartition, RecordQueue>();

            // initialize the topology with its own context
            this.processorContext = new ProcessorContext<object, object>(
                this.Context,
                id,
                this,
                config,
                this.recordCollector,
                stateMgr,
                cache);

            ITimestampExtractor defaultTimestampExtractor = config.GetDefaultTimestampExtractor(this.Context.Services);
            IDeserializationExceptionHandler defaultDeserializationExceptionHandler = config.GetDefaultDeserializationExceptionHandler(this.Context.Services);

            foreach (TopicPartition partition in partitions ?? Enumerable.Empty<TopicPartition>())
            {
                var source = topology.Source(partition.Topic);
                ITimestampExtractor? sourceTimestampExtractor = source.TimestampExtractor;
                ITimestampExtractor timestampExtractor = sourceTimestampExtractor ?? defaultTimestampExtractor;

                RecordQueue queue = new RecordQueue<object, object>(
                    partition,
                    source,
                    timestampExtractor,
                    defaultDeserializationExceptionHandler,
                    this.processorContext);

                partitionQueues.Put(partition, queue);
            }

            this.recordInfo = new RecordInfo();
            this.partitionGroup = new PartitionGroup(partitionQueues);

            stateMgr.RegisterGlobalStateStores(topology.globalStateStores);
        }


        public override bool IsActive()
        {
            return true;
        }

        /**
         * @throws LockException could happen when multi-threads within the single instance, could retry
         * @throws TimeoutException if initializing record collector timed out
         * @throws StreamsException fatal error, should close the thread
         */

        public override void InitializeIfNeeded()
        {
            if (this.CurrentState == TaskState.CREATED)
            {
                this.recordCollector.Initialize();

                StateManagerUtil.RegisterStateStores(this.log, this.logPrefix, this.topology, this.stateMgr, this.stateDirectory, this.processorContext);

                this.TransitionTo(TaskState.RESTORING);

                this.log.LogInformation("Initialized");
            }
        }

        /**
         * @throws TimeoutException if fetching committed offsets timed out
         */

        public override void CompleteRestoration()
        {
            if (this.CurrentState == TaskState.RESTORING)
            {
                this.InitializeMetadata();
                this.InitializeTopology();
                this.processorContext.Initialize();
                this.idleStartTime = RecordQueue.UNKNOWN;

                this.TransitionTo(TaskState.RUNNING);

                this.log.LogInformation("Restored and ready to run");
            }
            else
            {
                throw new InvalidOperationException("Illegal state " + this.CurrentState + " while completing restoration for active task " + this.Id);
            }
        }

        /**
         * <pre>
         * the following order must be followed:
         *  1. first close topology to make sure all cached records in the topology are processed
         *  2. then flush the state, send any left changelog records
         *  3. then flush the record collector
         *  4. then commit the record collector -- for EOS this is the synchronization barrier
         *  5. then checkpoint the state manager -- even if we crash before this step, EOS is still guaranteed
         * </pre>
         *
         * @throws TaskMigratedException if committing offsets failed (non-EOS)
         *                               or if the task producer got fenced (EOS)
         */

        public override void PrepareSuspend()
        {
            if (this.CurrentState == TaskState.CREATED || this.CurrentState == TaskState.SUSPENDED)
            {
                // do nothing
                this.log.LogTrace("Skip prepare suspending since state is {}", this.CurrentState);
            }
            else if (this.CurrentState == TaskState.RUNNING)
            {
                this.CloseTopology(true);

                this.stateMgr.Flush();
                this.recordCollector.Flush();

                this.log.LogInformation("Prepare suspending running");
            }
            else if (this.CurrentState == TaskState.RESTORING)
            {
                this.stateMgr.Flush();

                this.log.LogInformation("Prepare suspending restoring");
            }
            else
            {
                throw new InvalidOperationException("Illegal state " + this.CurrentState + " while suspending active task " + this.Id);
            }
        }

        public override void Suspend()
        {
            if (this.CurrentState == TaskState.CREATED || this.CurrentState == TaskState.SUSPENDED)
            {
                // do nothing
                this.log.LogTrace("Skip suspending since state is {}", this.CurrentState);
            }
            else if (this.CurrentState == TaskState.RUNNING)
            {
                this.stateMgr.Checkpoint(this.CheckpointableOffsets());
                this.partitionGroup.Clear();

                this.TransitionTo(TaskState.SUSPENDED);
                this.log.LogInformation("Suspended running");
            }
            else if (this.CurrentState == TaskState.RESTORING)
            {
                // we just checkpoint the position that we've restored up to without
                // going through the commit process
                this.stateMgr.Checkpoint(new Dictionary<TopicPartition, long>());

                // we should also clear any buffered records of a task when suspending it
                this.partitionGroup.Clear();

                this.TransitionTo(TaskState.SUSPENDED);
                this.log.LogInformation("Suspended restoring");
            }
            else
            {
                throw new InvalidOperationException("Illegal state " + this.CurrentState + " while suspending active task " + this.Id);
            }
        }

        /**
         * <pre>
         * - resume the task
         * </pre>
         */

        public override void Resume()
        {
            switch (this.CurrentState)
            {
                case TaskState.CREATED:
                case TaskState.RUNNING:
                case TaskState.RESTORING:
                    // no need to do anything, just let them continue running / restoring / closing
                    this.log.LogTrace("Skip resuming since state is {}", this.CurrentState);
                    break;

                case TaskState.SUSPENDED:
                    // just transit the state without any logical changes: suspended and restoring states
                    // are not actually any different for inner modules
                    this.TransitionTo(TaskState.RESTORING);
                    this.log.LogInformation("Resumed to restoring state");

                    break;

                default:
                    throw new InvalidOperationException("Illegal state " + this.CurrentState + " while resuming active task " + this.Id);
            }
        }


        public override void PrepareCommit()
        {
            switch (this.CurrentState)
            {
                case TaskState.RUNNING:
                case TaskState.RESTORING:
                    this.stateMgr.Flush();
                    this.recordCollector.Flush();

                    this.log.LogDebug("Prepared task for committing");

                    break;

                default:
                    throw new InvalidOperationException("Illegal state " + this.CurrentState + " while preparing active task " + this.Id + " for committing");
            }
        }


        public override void PostCommit()
        {
            switch (this.CurrentState)
            {
                case TaskState.RUNNING:
                    this.commitNeeded = false;
                    this.commitRequested = false;

                    if (!this.eosEnabled)
                    {
                        this.stateMgr.Checkpoint(this.CheckpointableOffsets());
                    }

                    this.log.LogDebug("Committed");

                    break;

                case TaskState.RESTORING:
                    this.commitNeeded = false;
                    this.commitRequested = false;

                    this.stateMgr.Checkpoint(this.CheckpointableOffsets());

                    this.log.LogDebug("Committed");

                    break;

                default:
                    throw new InvalidOperationException("Illegal state " + this.CurrentState + " while post committing active task " + this.Id);
            }
        }


        public Dictionary<TopicPartition, OffsetAndMetadata> CommittableOffsetsAndMetadata()
        {
            if (this.CurrentState == TaskState.CLOSED)
            {
                throw new InvalidOperationException("Task " + this.Id + " is closed.");
            }

            if (this.CurrentState != TaskState.RUNNING)
            {
                return new Dictionary<TopicPartition, OffsetAndMetadata>();
            }

            Dictionary<TopicPartition, long> partitionTimes = this.ExtractPartitionTimes();

            Dictionary<TopicPartition, OffsetAndMetadata> consumedOffsetsAndMetadata = new Dictionary<TopicPartition, OffsetAndMetadata>(this.consumedOffsets.Count);
            foreach (var entry in this.consumedOffsets)
            {
                TopicPartition partition = entry.Key;
                long? offset = this.partitionGroup.HeadRecordOffset(partition);
                if (offset == null)
                {
                    try
                    {
                        offset = this.mainConsumer.Position(partition);
                    }
                    catch (TimeoutException error)
                    {
                        // the `consumer.position()` call should never block, because we know that we did process data
                        // for the requested partition and thus the consumer should have a valid local position
                        // that it can return immediately

                        // hence, a `TimeoutException` indicates a bug and thus we rethrow it as fatal `InvalidOperationException`
                        throw new InvalidOperationException(error.Message);
                    }
                    catch (KafkaException fatal)
                    {
                        throw new StreamsException(fatal);
                    }
                }

                long partitionTime = partitionTimes[partition];
                consumedOffsetsAndMetadata.Put(partition, new OffsetAndMetadata(offset, EncodeTimestamp(partitionTime)));
            }

            return consumedOffsetsAndMetadata;
        }

        private Dictionary<TopicPartition, long> ExtractPartitionTimes()
        {
            Dictionary<TopicPartition, long> partitionTimes = new Dictionary<TopicPartition, long>();
            foreach (TopicPartition partition in this.partitionGroup.Partitions())
            {
                partitionTimes.Put(partition, this.partitionGroup.PartitionTimestamp(partition));
            }

            return partitionTimes;
        }

        public override Dictionary<TopicPartition, long> PrepareCloseClean()
        {
            Dictionary<TopicPartition, long> checkpoint = this.PrepareClose(true);

            this.log.LogInformation("Prepared clean close");

            return checkpoint;
        }


        public override void CloseClean(Dictionary<TopicPartition, long> checkpoint)
        {
            this.Close(true, checkpoint);

            this.log.LogInformation("Closed clean");
        }


        public override void PrepareCloseDirty()
        {
            this.PrepareClose(false);

            this.log.LogInformation("Prepared dirty close");
        }

        public override void CloseDirty()
        {
            this.Close(false, null);

            this.log.LogInformation("Closed dirty");
        }

        /**
         * <pre>
         * the following order must be followed:
         *  1. first close topology to make sure all cached records in the topology are processed
         *  2. then flush the state, send any left changelog records
         *  3. then flush the record collector
         * </pre>
         *
         * @param clean    shut down cleanly (ie, incl. flush) if {@code true} --
         *                 otherwise, just close open resources
         * @throws TaskMigratedException if the task producer got fenced (EOS)
         */
        private Dictionary<TopicPartition, long> PrepareClose(bool clean)
        {
            Dictionary<TopicPartition, long> checkpoint;

            if (this.CurrentState == TaskState.CREATED)
            {
                // the task is created and not initialized, just re-write the checkpoint file
                checkpoint = new Dictionary<TopicPartition, long>();
            }
            else if (this.CurrentState == TaskState.RUNNING)
            {
                this.CloseTopology(clean);

                if (clean)
                {
                    this.stateMgr.Flush();
                    this.recordCollector.Flush();
                    checkpoint = this.CheckpointableOffsets();
                }
                else
                {
                    checkpoint = null; // `null` indicates to not write a checkpoint
                    this.ExecuteAndMaybeSwallow(false, this.stateMgr.Flush, "state manager flush", this.log);
                }
            }
            else if (this.CurrentState == TaskState.RESTORING)
            {
                this.ExecuteAndMaybeSwallow(clean, this.stateMgr.Flush, "state manager flush", this.log);
                checkpoint = new Dictionary<TopicPartition, long>();
            }
            else if (this.CurrentState == TaskState.SUSPENDED)
            {
                // if `SUSPENDED` do not need to checkpoint, since when suspending we've already committed the state
                checkpoint = null; // `null` indicates to not write a checkpoint
            }
            else
            {
                throw new InvalidOperationException("Illegal state " + this.CurrentState + " while prepare closing active task " + this.Id);
            }

            return checkpoint;
        }

        /**
         * <pre>
         * the following order must be followed:
         *  1. checkpoint the state manager -- even if we crash before this step, EOS is still guaranteed
         *  2. then if we are closing on EOS and dirty, wipe out the state store directory
         *  3. finally release the state manager lock
         * </pre>
         */
        private void Close(bool clean,
                           Dictionary<TopicPartition, long> checkpoint)
        {
            if (clean && checkpoint != null)
            {
                this.ExecuteAndMaybeSwallow(clean, () => this.stateMgr.Checkpoint(checkpoint), "state manager checkpoint", this.log);
            }

            switch (this.CurrentState)
            {
                case TaskState.CREATED:
                case TaskState.RUNNING:
                case TaskState.RESTORING:
                case TaskState.SUSPENDED:
                    // first close state manager (which is idempotent) then close the record collector
                    // if the latter throws and we re-close dirty which would close the state manager again.
                    this.ExecuteAndMaybeSwallow(
                        clean,
                        () => StateManagerUtil.CloseStateManager(
                            this.log,
                            this.logPrefix,
                            clean,
                            this.eosEnabled,
                            this.stateMgr,
                            this.stateDirectory,
                            TaskType.ACTIVE
                        ),
                        "state manager close",
                        this.log);

                    // executeAndMaybeSwallow(clean, RecordCollector.Close, "record collector close", log);

                    break;

                default:
                    throw new InvalidOperationException("Illegal state " + this.CurrentState + " while closing active task " + this.Id);
            }

            this.partitionGroup.Close();
            // closeTaskSensor.record();

            this.TransitionTo(TaskState.CLOSED);
        }

        /**
         * An active task is processable if its buffer contains data for all of its input
         * source topic partitions, or if it is enforced to be processable
         */
        public bool IsProcessable(DateTime wallClockTime)
        {
            if (this.CurrentState == TaskState.CLOSED)
            {
                // a task is only closing / closed when 1) task manager is closing, 2) a rebalance is undergoing;
                // in either case we can just log it and move on without notifying the thread since the consumer
                // would soon be updated to not return any records for this task anymore.
                this.log.LogInformation("Stream task {} is already in {} state, skip processing it.", this.Id, this.CurrentState);

                return false;
            }

            if (this.partitionGroup.AllPartitionsBuffered())
            {
                this.idleStartTime = RecordQueue.UNKNOWN;
                return true;
            }
            else if (this.partitionGroup.NumBuffered() > 0)
            {
                if (this.idleStartTime == RecordQueue.UNKNOWN)
                {
                    this.idleStartTime = wallClockTime;
                }

                if ((wallClockTime - this.idleStartTime).TotalMilliseconds >= this.maxTaskIdle.TotalMilliseconds)
                {
                    // enforcedProcessingSensor.record();
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                // there's no data in any of the topics; we should reset the enforced
                // processing timer
                this.idleStartTime = RecordQueue.UNKNOWN;
                return false;
            }
        }

        /**
         * Process one record.
         *
         * @return true if this method processes a record, false if it does not process a record.
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */
        public bool Process(DateTime wallClockTime)
        {
            if (!this.IsProcessable(wallClockTime))
            {
                return false;
            }

            // get the next record to process
            StampedRecord record = null; //partitionGroup.NextRecord(recordInfo);

            // if there is no record to process, return immediately
            if (record == null)
            {
                return false;
            }

            try
            {
                // process the record by passing to the source node of the topology
                ProcessorNode<object, object> currNode = (ProcessorNode<object, object>)this.recordInfo.Node();
                TopicPartition partition = this.recordInfo.Partition();

                this.log.LogTrace("Start processing one record [{}]", record);

                this.UpdateProcessorContext(record, currNode);
                //MaybeMeasureLatency(()=>currNode.process(record.Key, record.Value), time, processLatencySensor);

                this.log.LogTrace("Completed processing one record [{}]", record);

                // update the consumed offset map after processing is done
                this.consumedOffsets.Put(partition, record.offset);
                this.commitNeeded = true;

                // after processing this record, if its partition queue's buffered size has been
                // decreased to the threshold, we can then resume the consumption on this partition
                if (this.recordInfo.queue.Size() == this.maxBufferedSize)
                {
                    this.mainConsumer.Resume(new List<TopicPartition> { partition });
                }
            }
            catch (StreamsException e)
            {
                throw e;
            }
            catch (RuntimeException e)
            {
                string stackTrace = this.GetStacktraceString(e);
                throw new StreamsException(string.Format("Exception caught in process. taskId=%s, " +
                                                      "processor=%s, topic=%s, partition=%d, offset=%d, stacktrace=%s",
                                                  this.Id,
                                                  this.processorContext.GetCurrentNode().Name,
                                                  record.Topic,
                                                  record.partition,
                                                  record.offset,
                                                  stackTrace
                ), e);
            }
            finally
            {
                this.processorContext.SetCurrentNode(null);
            }

            return true;
        }


        public void RecordProcessBatchTime(long processBatchTime)
        {
            this.processTime += TimeSpan.FromMilliseconds(processBatchTime);
        }


        public void RecordProcessTimeRatioAndBufferSize(long allTaskProcessMs)
        {
            // bufferedRecordsSensor.record(partitionGroup.numBuffered());
            // processRatioSensor.record((double)processTimeMs / allTaskProcessMs);
            this.processTime = TimeSpan.Zero;
        }

        private string GetStacktraceString(RuntimeException e)
        {
            string stacktrace = null;
            //try (StringWriter stringWriter = new StringWriter();
            //PrintWriter printWriter = new PrintWriter(stringWriter)) {
            //    e.printStackTrace(printWriter);
            //    stacktrace = stringWriter.toString();
            //} catch (IOException ioe)
            //{
            //    log.error("Encountered error extracting stacktrace from this exception", ioe);
            //}
            return stacktrace;
        }

        /**
         * @throws InvalidOperationException if the current node is not null
         * @throws TaskMigratedException if the task producer got fenced (EOS only)
         */

        public void Punctuate(
            IProcessorNode<object, object> node,
            DateTime timestamp,
            PunctuationType type,
            IPunctuator punctuator)
        {
            if (this.processorContext.GetCurrentNode() != null)
            {
                throw new InvalidOperationException(string.Format("%sCurrent node is not null", this.logPrefix));
            }

            this.UpdateProcessorContext(new StampedRecord(DUMMY_RECORD, timestamp), node);

            if (this.log.IsEnabled(LogLevel.Trace))
            {
                this.log.LogTrace("Punctuating processor {} with timestamp {} and punctuation type {}", node.Name, timestamp, type);
            }

            try
            {
                //maybeMeasureLatency(() => node.punctuate(timestamp, punctuator), time, punctuateLatencySensor);
            }
            catch (StreamsException e)
            {
                throw e;
            }
            catch (RuntimeException e)
            {
                throw new StreamsException(string.Format("%sException caught while punctuating processor '%s'", this.logPrefix, node.Name), e);
            }
            finally
            {
                this.processorContext.SetCurrentNode(null);
            }
        }

        private void UpdateProcessorContext(StampedRecord record, IProcessorNode currNode)
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
         * Return all the checkpointable offsets(written + consumed) to the state manager.
         * Currently only changelog topic offsets need to be checkpointed.
         */
        private Dictionary<TopicPartition, long> CheckpointableOffsets()
        {
            Dictionary<TopicPartition, long> checkpointableOffsets = new Dictionary<TopicPartition, long>(this.recordCollector.offsets);
            foreach (var entry in this.consumedOffsets)
            {
                checkpointableOffsets.TryAdd(entry.Key, entry.Value);
            }

            return checkpointableOffsets;
        }

        private void InitializeMetadata()
        {
            try
            {
                Dictionary<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = this.mainConsumer.Committed(this.Partitions, TimeSpan.MaxValue)
                    //.Stream()
                    .Where(e => e != null)
                    .ToDictionary(kvp => kvp.TopicPartition, kvp => new OffsetAndMetadata(kvp.Offset));

                this.InitializeTaskTime(offsetsAndMetadata);
            }
            catch (TimeoutException e)
            {
                this.log.LogWarning("Encountered {} while trying to fetch committed offsets, will retry initializing the metadata in the next loop." +
                    "\nConsider overwriting consumer config {} to a larger value to avoid timeout errors",
                    e.ToString());
                //ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);

                throw;
            }
            catch (KafkaException e)
            {
                throw new StreamsException(string.Format("task [%s] Failed to initialize offsets for %s", this.Id, this.Partitions), e);
            }
        }

        private void InitializeTaskTime(Dictionary<TopicPartition, OffsetAndMetadata> offsetsAndMetadata)
        {
            foreach (var entry in offsetsAndMetadata)
            {
                TopicPartition partition = entry.Key;
                OffsetAndMetadata metadata = entry.Value;

                if (metadata != null)
                {
                    var committedTimestamp = this.DecodeTimestamp(metadata.ToString());
                    this.partitionGroup.SetPartitionTime(partition, committedTimestamp);
                    this.log.LogDebug("A committed timestamp was detected: setting the partition time of partition {}"
                        + " to {} in stream task {}", partition, committedTimestamp, this.Id);
                }
                else
                {
                    this.log.LogDebug("No committed timestamp was found in metadata for partition {}", partition);
                }
            }

            HashSet<TopicPartition> nonCommitted = new HashSet<TopicPartition>(this.Partitions);
            nonCommitted.ExceptWith(offsetsAndMetadata.Keys);
            foreach (TopicPartition partition in nonCommitted)
            {
                this.log.LogDebug("No committed offset for partition {}, therefore no timestamp can be found for this partition", partition);
            }
        }


        public Dictionary<TopicPartition, long> PurgeableOffsets()
        {
            Dictionary<TopicPartition, long> purgeableConsumedOffsets = new Dictionary<TopicPartition, long>();
            foreach (var entry in this.consumedOffsets)
            {
                TopicPartition tp = entry.Key;
                if (this.topology.IsRepartitionTopic(tp.Topic))
                {
                    purgeableConsumedOffsets.Put(tp, entry.Value + 1);
                }
            }

            return purgeableConsumedOffsets;
        }

        private void InitializeTopology()
        {
            // initialize the task by initializing all its processor nodes in the topology
            this.log.LogTrace("Initializing processor nodes of the topology");
            foreach (var node in this.topology.Processors())
            {
                this.processorContext.SetCurrentNode(node);
                try
                {
                    // node.Init(processorContext);
                }
                finally
                {
                    this.processorContext.SetCurrentNode(null);
                }
            }
        }

        private void CloseTopology(bool clean)
        {
            this.log.LogTrace("Closing processor topology");

            // close the processors
            // make sure close() is called for each node even when there is a RuntimeException
            RuntimeException exception = null;
            foreach (var node in this.topology.Processors())
            {
                this.processorContext.SetCurrentNode(node);
                try
                {
                    // node.Close();
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

            if (exception != null && clean)
            {
                throw exception;
            }
        }

        /**
         * Adds records to queues. If a record has an invalid (i.e., negative) timestamp, the record is skipped
         * and not added to the queue for processing
         *
         * @param partition the partition
         * @param records   the records
         */

        public override void AddRecords(TopicPartition partition, IEnumerable<ConsumeResult<byte[], byte[]>> records)
        {
            int newQueueSize = this.partitionGroup.AddRawRecords(partition, records);

            if (this.log.IsEnabled(LogLevel.Trace))
            {
                this.log.LogTrace("Added records into the buffered queue of partition {}, new queue size is {}", partition, newQueueSize);
            }

            // if after adding these records, its partition queue's buffered size has been
            // increased beyond the threshold, we can then pause the consumption for this partition
            if (newQueueSize > this.maxBufferedSize)
            {
                this.mainConsumer.Pause(new List<TopicPartition> { partition });
            }
        }

        /**
         * Schedules a punctuation for the processor
         *
         * @param interval the interval in milliseconds
         * @param type     the punctuation type
         * @throws InvalidOperationException if the current node is not null
         */
        public ICancellable Schedule(TimeSpan interval, PunctuationType type, Action<DateTime> punctuator)
        {
            switch (type)
            {
                case PunctuationType.STREAM_TIME:
                    // align punctuation to 0L, punctuate as soon as we have data
                    return this.Schedule(DateTime.MinValue, interval, type, punctuator);
                case PunctuationType.WALL_CLOCK_TIME:
                    // align punctuation to now, punctuate after interval has elapsed
                    return this.Schedule(this.Context.Clock.UtcNow + interval, interval, type, punctuator);
                default:
                    throw new ArgumentException("Unrecognized PunctuationType: " + type);
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
        private ICancellable Schedule(DateTime startTime, TimeSpan interval, PunctuationType type, Action<DateTime> punctuator)
        {
            if (this.processorContext.GetCurrentNode() == null)
            {
                throw new InvalidOperationException(string.Format("%sCurrent node is null", this.logPrefix));
            }

            PunctuationSchedule schedule = new PunctuationSchedule(this.processorContext.GetCurrentNode(), startTime, interval, punctuator);

            switch (type)
            {
                case PunctuationType.STREAM_TIME:
                    // STREAM_TIME punctuation is data driven, will first punctuate as soon as stream-time is known and >= time,
                    // stream-time is known when we have received at least one record from each input topic
                    return this.streamTimePunctuationQueue.Schedule(schedule);
                case PunctuationType.WALL_CLOCK_TIME:
                    // WALL_CLOCK_TIME is driven by the wall clock time, will first punctuate when now >= time
                    return this.systemTimePunctuationQueue.Schedule(schedule);
                default:
                    throw new ArgumentException("Unrecognized PunctuationType: " + type);
            }
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
                bool punctuated = this.streamTimePunctuationQueue.MayPunctuate(
                    streamTime,
                    PunctuationType.STREAM_TIME,
                    this);

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
            var systemTime = this.Context.Clock.UtcNow;

            bool punctuated = this.systemTimePunctuationQueue.MayPunctuate(
                systemTime,
                PunctuationType.WALL_CLOCK_TIME,
                this);

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

        private static string EncodeTimestamp(long partitionTime)
        {
            ByteBuffer buffer = new ByteBuffer().Allocate(9);
            buffer.PutInt(LATEST_MAGIC_BYTE);
            buffer.PutLong(partitionTime);
            return ""; // Base64.getEncoder().encodeToString(buffer.array());
        }

        private DateTime DecodeTimestamp(string encryptedString)
        {
            if (encryptedString.IsEmpty())
            {
                return RecordQueue.UNKNOWN;
            }

            ByteBuffer buffer = new ByteBuffer().Wrap(Array.Empty<byte>());// Base64.getDecoder().decode(encryptedString));
            var bytes = new byte[1];
            buffer.Get(bytes);

            var version = bytes[0];
            switch (version)
            {
                case LATEST_MAGIC_BYTE:
                    return Timestamp.UnixTimestampMsToDateTime(buffer.GetLong());
                default:
                    this.log.LogWarning("Unsupported offset metadata version found. Supported version {}. Found version {}.",
                             LATEST_MAGIC_BYTE, version);
                    return RecordQueue.UNKNOWN;
            }
        }

        public IProcessorContext ProcessorContext()
        {
            return this.processorContext;
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

            // print assigned partitions
            if (this.Partitions != null && !this.Partitions.IsEmpty())
            {
                sb.Append(indent).Append("Partitions [");
                foreach (TopicPartition topicPartition in this.Partitions)
                {
                    sb.Append(topicPartition).Append(", ");
                }

                sb.Length -= 2;
                sb.Append("]\n");
            }
            return sb.ToString();
        }

        public override Dictionary<TopicPartition, long> ChangelogOffsets()
        {
            if (this.CurrentState == TaskState.RUNNING)
            {
                // if we are in running state, just return the latest offset sentinel indicating
                // we should be at the end of the changelog
                return this.ChangelogPartitions()
                    .ToDictionary(kvp => new TopicPartition(kvp.Topic, kvp.Partition), kvp => this.LatestOffset);
            }
            else
            {
                return this.stateMgr.ChangelogPartitions().ToDictionary(p => p, p => this.LatestOffset);
            }
        }

        public bool HasRecordsQueued()
        {
            return this.NumBuffered() > 0;
        }

        private int NumBuffered()
        {
            return this.partitionGroup.NumBuffered();
        }

        private DateTime StreamTime()
        {
            return this.partitionGroup.streamTime;
        }

        public override bool CommitNeeded()
        {
            throw new NotImplementedException();
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
    }
}
