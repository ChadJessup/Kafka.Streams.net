using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals.Metrics;
using Kafka.Streams.KStream;
using Kafka.Streams.State.Internals;
using System;
using System.Collections.Generic;
using System.IO;

namespace Kafka.Streams.Processor.Internals
{
    public abstract class AbstractProcessorContext<K, V> : IInternalProcessorContext<K, V>
    {
        public static string NONEXIST_TOPIC = "__null_topic__";
        private readonly StreamsConfig config;
        private readonly ThreadCache cache;
        private bool initialized;
        public ProcessorRecordContext recordContext { get; protected set; }
        public ProcessorNode<K, V> currentNode { get; protected set; }
        protected IStateManager stateManager { get; }

        public IStreamsMetrics metrics { get; }
        public TaskId taskId { get; }
        public string applicationId { get; }
        public ISerde<K> keySerde { get; }
        public ISerde<V> valueSerde { get; }

        public AbstractProcessorContext(
            TaskId taskId,
            StreamsConfig config,
            StreamsMetricsImpl metrics,
            IStateManager stateManager,
            ThreadCache cache)
        {
            this.taskId = taskId;
            this.applicationId = config.getString(StreamsConfigPropertyNames.ApplicationId);
            this.config = config;
            this.metrics = metrics;
            this.stateManager = stateManager;
            valueSerde = (ISerde<V>)config.defaultValueSerde();
            keySerde = (ISerde<K>)config.defaultKeySerde();
            this.cache = cache;
        }

        public DirectoryInfo stateDir => stateManager.baseDir;

        public virtual void register(
            IStateStore store,
            IStateRestoreCallback stateRestoreCallback)
        {
            if (initialized)
            {
                throw new InvalidOperationException("Can only create state stores during initialization.");
            }

            store = store ?? throw new ArgumentNullException(nameof(store));

            stateManager.register(store, stateRestoreCallback);
        }

        /**
         * @throws InvalidOperationException if the task's record is null
         */
        public string Topic
        {
            get
            {
                if (recordContext == null)
                {
                    throw new InvalidOperationException("This should not happen as Topic should only be called while a record is processed");
                }

                string topic = recordContext.Topic;

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
            Dictionary<string, object> combined = new Dictionary<string, object>();
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

        public abstract void setCurrentNode(ProcessorNode<K, V> currentNode);
        //{
        //    this.currentNode = currentNode;
        //}

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

        public ICancellable schedule(TimeSpan interval, PunctuationType type, Punctuator callback)
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
    }
}