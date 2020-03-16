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
            keySerde = (ISerde<K>)config.defaultKeySerde();
            valueSerde = (ISerde<V>)config.defaultValueSerde();
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

        public DirectoryInfo stateDir => stateManager.baseDir;

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
                    throw new InvalidOperationException("This should not happen as partition() should only be called while a record is processed");
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
                    throw new InvalidOperationException("This should not happen as headers() should only be called while a record is processed");
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

        public Dictionary<string, object> appConfigs()
        {
            var combined = new Dictionary<string, object>();
            //combined.putAll(config.originals());
            //combined.putAll(config.Values);
            return combined;
        }

        public Dictionary<string, object> appConfigsWithPrefix(string prefix)
        {
            return null; // config.originalsWithPrefix(prefix);
        }

        public virtual void setRecordContext(ProcessorRecordContext recordContext)
        {
            this.recordContext = recordContext;
        }

        public virtual void SetCurrentNode(IProcessorNode current)
            => this.currentNode = current;

        public ThreadCache getCache()
        {
            return cache;
        }

        public void initialize()
        {
            initialized = true;
        }

        public void uninitialize()
        {
            initialized = false;
        }

        public virtual IStateStore getStateStore(string name)
        {
            throw new NotImplementedException();
        }

        public virtual ICancellable schedule(TimeSpan interval, PunctuationType type, IPunctuator callback)
        {
            throw new NotImplementedException();
        }

        public virtual void forward<K1, V1>(K1 key, V1 value)
        {
            throw new NotImplementedException();
        }

        public virtual void forward<K1, V1>(K1 key, V1 value, To to)
        {
            throw new NotImplementedException();
        }

        public virtual void forward<K1, V1>(K1 key, V1 value, string childName)
        {
            throw new NotImplementedException();
        }

        public virtual void commit()
        {
            throw new NotImplementedException();
        }

        public ICancellable schedule(TimeSpan interval, PunctuationType type, Action<long> callback)
        {
            throw new NotImplementedException();
        }
    }
}