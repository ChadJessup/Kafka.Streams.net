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
            KafkaStreamsContext context,
            TaskId taskId,
            StreamsConfig config,
            IStateManager stateManager,
            ThreadCache cache)
            : base(
                context,
                taskId,
                config,
                stateManager,
                cache)
        {
            this.KeySerde = config.GetDefaultKeySerde(context);
            this.ValueSerde = config.GetDefaultValueSerde(context);
        }
    }

    public abstract class AbstractProcessorContext : IInternalProcessorContext
    {
        public const string NONEXIST_TOPIC = "__null_topic__";
        private readonly StreamsConfig config;
        private readonly ThreadCache cache;
        private bool initialized;
        public virtual ProcessorRecordContext RecordContext { get; protected set; }
        public IProcessorNode GetCurrentNode() => this.CurrentNode;
        public virtual IProcessorNode CurrentNode { get; set; }
        protected IStateManager StateManager { get; }
        public KafkaStreamsContext Context { get; }
        public TaskId TaskId { get; }
        public string ApplicationId { get; }

        public AbstractProcessorContext(
            KafkaStreamsContext context,
            TaskId taskId,
            StreamsConfig config,
            IStateManager stateManager,
            ThreadCache cache)
        {
            this.Context = context;
            this.TaskId = taskId;
            this.ApplicationId = config.ApplicationId;
            this.config = config;
            this.StateManager = stateManager;
            this.cache = cache;
        }

        public DirectoryInfo StateDir => this.StateManager.BaseDir;

        public virtual void Register(
            IStateStore store,
            IStateRestoreCallback stateRestoreCallback)
        {
            if (this.initialized)
            {
                throw new InvalidOperationException("Can only create state stores during initialization.");
            }

            store = store ?? throw new ArgumentNullException(nameof(store));

            this.StateManager.Register(store, stateRestoreCallback);
        }

        /**
         * @throws InvalidOperationException if the task's record is null
         */
        public virtual string Topic
        {
            get
            {
                if (this.RecordContext == null)
                {
                    throw new InvalidOperationException("This should not happen as Topic should only be called while a record is processed");
                }

                var topic = this.RecordContext.Topic;

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
        public virtual int Partition
        {
            get
            {
                if (this.RecordContext == null)
                {
                    throw new InvalidOperationException("This should not happen as Partition should only be called while a record is processed");
                }

                return this.RecordContext.Partition;
            }
        }

        /**
         * @throws InvalidOperationException if offset is null
         */
        public virtual long Offset
        {
            get
            {
                if (this.RecordContext == null)
                {
                    throw new InvalidOperationException("This should not happen as offset() should only be called while a record is processed");
                }

                return this.RecordContext.Offset;
            }
        }

        public virtual Headers Headers
        {
            get
            {
                if (this.RecordContext == null)
                {
                    throw new InvalidOperationException("This should not happen as Headers should only be called while a record is processed");
                }

                return this.RecordContext.Headers;
            }
        }

        /**
         * @throws InvalidOperationException if timestamp is null
         */
        public virtual DateTime Timestamp
        {
            get
            {
                if (this.RecordContext == null)
                {
                    throw new InvalidOperationException("This should not happen as timestamp() should only be called while a record is processed");
                }

                return this.RecordContext.Timestamp;
            }
        }

        public ISerde KeySerde { get; protected set; }
        public ISerde ValueSerde { get; protected set; }

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
            this.RecordContext = recordContext;
        }

        public virtual void SetCurrentNode(IProcessorNode? current)
            => this.CurrentNode = current;

        public ThreadCache GetCache()
        {
            return this.cache;
        }

        public void Initialize()
        {
            this.initialized = true;
        }

        public void Uninitialize()
        {
            this.initialized = false;
        }

        public virtual IStateStore GetStateStore(string Name)
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

        public ICancellable Schedule(TimeSpan interval, PunctuationType type, Action<DateTime> callback)
        {
            throw new NotImplementedException();
        }

        public ICancellable Schedule(TimeSpan interval, PunctuationType? type, IPunctuator? callback)
        {
            throw new NotImplementedException();
        }
    }
}
