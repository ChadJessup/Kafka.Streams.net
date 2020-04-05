using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;
using System.IO;

namespace Kafka.Streams.Processors
{
    public abstract class AbstractProcessorContext<K, V> : AbstractProcessorContext
    {
        public AbstractProcessorContext(
            TaskId taskId,
            StreamsConfig config,
            IStateManager stateManager,
            ThreadCache cache)
            : base(
                taskId,
                config,
                stateManager,
                cache)
        {
            keySerde = (ISerde<K>)config.GetDefaultKeySerde();
            valueSerde = (ISerde<V>)config.GetDefaultValueSerde();
        }

        public ISerde<K> keySerde { get; }
        public ISerde<V> valueSerde { get; }
    }

    public abstract class AbstractProcessorContext : IInternalProcessorContext
    {
        public const string NONEXIST_TOPIC = "__null_topic__";
        private readonly StreamsConfig config;
        private readonly ThreadCache cache;
        private bool initialized;
        public virtual ProcessorRecordContext recordContext { get; protected set; }

        public virtual IProcessorNode currentNode { get; set; }

        public IProcessorNode GetCurrentNode()
            => this.currentNode;

        protected IStateManager stateManager { get; }

        public TaskId taskId { get; }
        public string applicationId { get; }

        public AbstractProcessorContext(
            TaskId taskId,
            StreamsConfig config,
            IStateManager stateManager,
            ThreadCache cache)
        {
            this.taskId = taskId;
            this.applicationId = config.ApplicationId;
            this.config = config;
            this.stateManager = stateManager;
            this.cache = cache;
        }

        public DirectoryInfo stateDir => stateManager.BaseDir;

        public virtual void Register(
            IStateStore store,
            IStateRestoreCallback stateRestoreCallback)
        {
            if (initialized)
            {
                throw new InvalidOperationException("Can only create state stores during initialization.");
            }

            store = store ?? throw new ArgumentNullException(nameof(store));

            stateManager.Register(store, stateRestoreCallback);
        }

        /**
         * @throws InvalidOperationException if the task's record is null
         */
        public virtual string Topic
        {
            get
            {
                if (recordContext == null)
                {
                    throw new InvalidOperationException("This should not happen as Topic should only be called while a record is processed");
                }

                var topic = recordContext.Topic;

                if (topic.Equals(NONEXIST_TOPIC))
                {
                    return null;
                }

                return topic;
            }
        }

        /**
         * @throws InvalidOperationException if partition is null
         */
        public virtual int partition
        {
            get
            {
                if (recordContext == null)
                {
                    throw new InvalidOperationException("This should not happen as Partition should only be called while a record is processed");
                }

                return recordContext.partition;
            }
        }

        /**
         * @throws InvalidOperationException if offset is null
         */
        public virtual long offset
        {
            get
            {
                if (recordContext == null)
                {
                    throw new InvalidOperationException("This should not happen as offset() should only be called while a record is processed");
                }

                return recordContext.offset;
            }
        }

        public virtual Headers headers
        {
            get
            {
                if (recordContext == null)
                {
                    throw new InvalidOperationException("This should not happen as Headers should only be called while a record is processed");
                }

                return recordContext.headers;
            }
        }

        /**
         * @throws InvalidOperationException if timestamp is null
         */
        public virtual long timestamp
        {
            get
            {
                if (recordContext == null)
                {
                    throw new InvalidOperationException("This should not happen as timestamp() should only be called while a record is processed");
                }

                return recordContext.timestamp;
            }
        }

        public ISerde keySerde { get; }
        public ISerde valueSerde { get; }

        public Dictionary<string, object> AppConfigs()
        {
            var combined = new Dictionary<string, object>();
            //combined.putAll(config.originals());
            //combined.putAll(config.Values);
            return combined;
        }

        public Dictionary<string, object> AppConfigsWithPrefix(string prefix)
        {
            return null; // config.originalsWithPrefix(prefix);
        }

        public virtual void SetRecordContext(ProcessorRecordContext recordContext)
        {
            this.recordContext = recordContext;
        }

        public virtual void SetCurrentNode(IProcessorNode? current)
            => this.currentNode = current;

        public ThreadCache GetCache()
        {
            return cache;
        }

        public void Initialize()
        {
            initialized = true;
        }

        public void Uninitialize()
        {
            initialized = false;
        }

        public virtual IStateStore GetStateStore(string name)
        {
            throw new NotImplementedException();
        }

        public virtual ICancellable Schedule(TimeSpan interval, PunctuationType type, IPunctuator callback)
        {
            throw new NotImplementedException();
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value)
        {
            throw new NotImplementedException();
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value, To to)
        {
            throw new NotImplementedException();
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value, string childName)
        {
            throw new NotImplementedException();
        }

        public virtual void Commit()
        {
            throw new NotImplementedException();
        }

        public ICancellable Schedule(TimeSpan interval, PunctuationType type, Action<long> callback)
        {
            throw new NotImplementedException();
        }
    }
}