using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Tasks
{
    public abstract class AbstractTask : ITask
    {
        public TaskId id { get; }
        public string applicationId { get; }
        public ProcessorTopology topology { get; }
        public ProcessorStateManager StateMgr { get; }
        public HashSet<TopicPartition> partitions { get; }
        public IConsumer<byte[], byte[]> consumer { get; }
        protected string logPrefix { get; }
        protected bool eosEnabled { get; }
        protected ILogger log { get; }
        public LogContext logContext { get; }

        readonly StateDirectory stateDirectory;

        protected bool TaskInitialized { get; set; }
        protected bool TaskClosed { get; set; }
        public bool commitNeeded { get; set; }

        protected IInternalProcessorContext processorContext { get; set; }

        /**
         * @throws ProcessorStateException if the state manager cannot be created
         */
        public AbstractTask(
            TaskId id,
            List<TopicPartition> partitions,
            ProcessorTopology topology,
            IConsumer<byte[], byte[]> consumer,
            IChangelogReader changelogReader,
            bool isStandby,
            StateDirectory stateDirectory,
            StreamsConfig config)
        {
            this.id = id;
            this.applicationId = config.ApplicationId;
            this.partitions = new HashSet<TopicPartition>(partitions);
            this.topology = topology;
            this.consumer = consumer;
            this.eosEnabled = StreamsConfigPropertyNames.ExactlyOnce.Equals(config.getString(StreamsConfigPropertyNames.ProcessingGuarantee));
            this.stateDirectory = stateDirectory;

            this.logPrefix = $"{(isStandby ? "standby-task" : "task")} [{id}] ";
            this.logContext = new LogContext(logPrefix);
            this.log = logContext.logger(GetType());

            // create the processor state manager
            try
            {
                StateMgr = new ProcessorStateManager(
                    id,
                    partitions,
                    isStandby,
                    stateDirectory,
                    topology.StoreToChangelogTopic,
                    changelogReader,
                    eosEnabled,
                    logContext);
            }
            catch (IOException e)
            {
                throw new ProcessorStateException($"{logPrefix}Error while creating the state manager", e);
            }
        }

        public IProcessorContext context => processorContext;

        public virtual IStateStore getStore(string name)
        {
            return StateMgr.GetStore(name);
        }

        /**
         * Produces a string representation containing useful information about a Task.
         * This is useful in debugging scenarios.
         *
         * @return A string representation of the StreamTask instance.
         */
        public override string ToString()
        {
            return ToString("");
        }

        public bool isEosEnabled()
        {
            return eosEnabled;
        }

        /**
         * Produces a string representation containing useful information about a Task starting with the given indent.
         * This is useful in debugging scenarios.
         *
         * @return A string representation of the Task instance.
         */
        public virtual string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append(indent);
            sb.Append("TaskId: ");
            sb.Append(id);
            sb.Append("\n");

            // print topology
            if (topology != null)
            {
                sb.Append(indent).Append(topology.ToString(indent + "\t"));
            }

            // print assigned partitions
            if (partitions != null && partitions.Any())
            {
                sb.Append(indent).Append("Partitions [");
                foreach (TopicPartition topicPartition in partitions)
                {
                    sb.Append(topicPartition.ToString()).Append(", ");
                }

                sb.Length -= 2;
                sb.Append("]\n");
            }

            return sb.ToString();
        }

        public virtual Dictionary<TopicPartition, long> activeTaskCheckpointableOffsets()
        {
            return new Dictionary<TopicPartition, long>();
        }

        protected virtual void UpdateOffsetLimits()
        {
            foreach (TopicPartition partition in partitions)
            {
                try
                {

                    //                OffsetAndMetadata metadata = consumer.Committed(partition); // TODO: batch API?
                    //              long offset = metadata != null ? metadata.offset : 0L;
                    //            stateMgr.putOffsetLimit(partition, offset);

                    //                    log.LogTrace("Updating store offset limits {} for changelog {}", offset, partition);
                }
                catch (AuthorizationException)
                {
                    //                  throw new ProcessorStateException(string.Format("task [%s] AuthorizationException when initializing offsets for %s", id, partition), e);
                }
                catch (WakeupException)
                {
                    throw;
                }
                catch (KafkaException)
                {
                    //                    throw new ProcessorStateException(string.Format("task [%s] Failed to initialize offsets for %s", id, partition), e);
                }
            }
        }

        /**
         * Flush all state stores owned by this task
         */
        protected virtual void flushState()
        {
            StateMgr.Flush();
        }

        /**
         * Package-private for testing only
         *
         * @throws StreamsException If the store's change log does not contain the partition
         */
        protected virtual void registerStateStores()
        {
            if (!topology.StateStores.Any())
            {
                return;
            }

            try
            {

                //if (!stateDirectory.@lock(id))
                //{
                //    throw new LockException(string.Format("%sFailed to lock the state directory for task %s", logPrefix, id));
                //}
            }
            catch (IOException e)
            {
                throw new StreamsException(
                    string.Format("%sFatal error while trying to lock the state directory for task %s",
                    logPrefix, id));
            }

            log.LogTrace("Initializing state stores");

            // set initial offset limits
            UpdateOffsetLimits();

            foreach (IStateStore store in topology.StateStores)
            {
                log.LogTrace("Initializing store {}", store.name);
                processorContext.uninitialize();
                store.init(processorContext, store);
            }
        }

        public virtual void reinitializeStateStoresForPartitions(List<TopicPartition> partitions)
        {
            StateMgr.ReinitializeStateStoresForPartitions(partitions, processorContext);
        }

        /**
         * @throws ProcessorStateException if there is an error while closing the state manager
         */
        public virtual void closeStateManager(bool clean)
        {
            ProcessorStateException exception = null;
            log.LogTrace("Closing state manager");
            try
            {

                StateMgr.Close(clean);
            }
            catch (ProcessorStateException e)
            {
                exception = e;
            }
            finally
            {

                try
                {
                    stateDirectory.Unlock(id);
                }
                catch (IOException e)
                {
                    if (exception == null)
                    {
                        exception = new ProcessorStateException($"{logPrefix}Failed to release state dir lock", e);
                    }
                }
            }

            if (exception != null)
            {
                throw exception;
            }
        }

        public virtual bool isClosed()
        {
            return TaskClosed;
        }

        public virtual bool hasStateStores()
        {
            return topology.StateStores.Any();
        }

        public abstract bool initializeStateStores();

        public virtual void initializeTopology()
        {
        }

        public virtual void commit()
        {
        }

        public virtual void suspend()
        {
        }

        public virtual void resume()
        {
        }

        public virtual void closeSuspended(bool clean, bool isZombie, RuntimeException e)
        {
        }

        public virtual void close(bool clean, bool isZombie)
        {
        }

        public IEnumerable<TopicPartition> changelogPartitions
            => new List<TopicPartition>();
    }
}