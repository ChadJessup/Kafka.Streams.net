using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Internals;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Windowed;
using Kafka.Streams.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Processors.Internals
{
    public static class ProcessorContext
    {
        internal static readonly To SEND_TO_ALL = To.All();
        public const string NONEXIST_TOPIC = AbstractProcessorContext.NONEXIST_TOPIC;
    }

    public class ProcessorContext<K, V> : AbstractProcessorContext<K, V>, ISupplier
        where V : class
    {
        private readonly StreamTask task;
        private readonly IRecordCollector collector;
        private readonly ToInternal toInternal = new ToInternal();

        public ProcessorContext(
            KafkaStreamsContext context,
            TaskId id,
            StreamTask task,
            StreamsConfig config,
            IRecordCollector collector,
            ProcessorStateManager stateMgr,
            ThreadCache cache)
            : base(
                  context,
                  id,
                  config,
                  stateMgr,
                  cache)
        {
            this.task = task;
            this.collector = collector;
        }

        public ProcessorStateManager GetStateMgr()
        {
            return (ProcessorStateManager)this.StateManager;
        }

        public IRecordCollector RecordCollector()
        {
            return this.collector;
        }

        /**
         * @throws StreamsException if an attempt is made to access this state store from an unknown node
         */
        public override IStateStore GetStateStore(string Name)
        {
            if (this.GetCurrentNode() == null)
            {
                throw new StreamsException("Accessing from an unknown node");
            }

            IStateStore? global = this.StateManager.GetGlobalStore(Name);
            if (global != null)
            {
                if (global is ITimestampedKeyValueStore<K, V>)
                {
                    return new TimestampedKeyValueStoreReadOnlyDecorator<K, V>(this.Context, (ITimestampedKeyValueStore<K, V>)global);
                }
                else if (global is IKeyValueStore<K, V>)
                {
                    return new KeyValueStoreReadOnlyDecorator<K, V>(this.Context, (IKeyValueStore<K, V>)global);
                }
                else if (global is ITimestampedWindowStore<K, V>)
                {
                    return new TimestampedWindowStoreReadOnlyDecorator<K, V>(this.Context, (ITimestampedWindowStore<K, V>)global);
                }
                else if (global is IWindowStore<K, V>)
                {
                    return new WindowStoreReadOnlyDecorator<K, V>(this.Context, (IWindowStore<K, V>)global);
                }
                else if (global is ISessionStore<K, V>)
                {
                    return new SessionStoreReadOnlyDecorator<K, V>(this.Context, (ISessionStore<K, V>)global);
                }

                return global;
            }

            if (!this.GetCurrentNode().StateStores.Contains(Name))
            {
                throw new StreamsException("IProcessor " + this.GetCurrentNode().Name + " has no access to IStateStore " + Name +
                    " as the store is not connected to the processor. If you.Add stores manually via '.AddStateStore()' " +
                    "make sure to connect the.Added store to the processor by providing the processor Name to " +
                    "'.AddStateStore()' or connect them via '.connectProcessorAndStateStores()'. " +
                    "DSL users need to provide the store Name to '.process()', '.transform()', or '.transformValues()' " +
                    "to connect the store to the corresponding operator. If you do not.Add stores manually, " +
                    "please file a bug report at https://issues.apache.org/jira/projects/KAFKA.");
            }

            IStateStore? store = this.StateManager.GetStore(Name);

            if (store is ITimestampedKeyValueStore<K, V>)
            {
                return new TimestampedKeyValueStoreReadWriteDecorator<K, V>(this.Context, (ITimestampedKeyValueStore<K, V>)store);
            }
            else if (store is IKeyValueStore<K, V>)
            {
                return new KeyValueStoreReadWriteDecorator<K, V>(this.Context, (IKeyValueStore<K, V>)store);
            }
            else if (store is ITimestampedWindowStore<K, V>)
            {
                return new TimestampedWindowStoreReadWriteDecorator<K, V>(this.Context, (ITimestampedWindowStore<K, V>)store);
            }
            else if (store is IWindowStore<K, V>)
            {
                return new WindowStoreReadWriteDecorator<K, V>(this.Context, (IWindowStore<K, V>)store);
            }
            else if (store is ISessionStore<K, V>)
            {
                return new SessionStoreReadWriteDecorator<K, V>(this.Context, (ISessionStore<K, V>)store);
            }

            return store;
        }

        public override void Forward<K1, V1>(K1 key, V1 value)
        {
            this.Forward(key, value, ProcessorContext.SEND_TO_ALL);
        }

        public override void Forward<K1, V1>(K1 key, V1 value, To to)
        {
            var previousNode = this.GetCurrentNode();
            ProcessorRecordContext previousContext = this.RecordContext;

            try
            {
                this.toInternal.Update(to);
                if (this.toInternal.HasTimestamp())
                {
                    this.RecordContext = new ProcessorRecordContext(
                        this.toInternal.Timestamp,
                        this.RecordContext.offset,
                        this.RecordContext.partition,
                        this.RecordContext.Topic,
                        this.RecordContext.headers);
                }

                var sendTo = this.toInternal.Child();
                if (sendTo == null)
                {
                    var children = new List<IProcessorNode<K, V>>(this.GetCurrentNode().Children.Select(c => (IProcessorNode<K, V>)c));
                    foreach (var child in children)
                    {
                        //forward(child, key, value);
                    }
                }
                else
                {
                    var child = this.GetCurrentNode().GetChild(sendTo);
                    if (child == null)
                    {
                        throw new StreamsException("Unknown downstream node: " + sendTo
                            + " either does not exist or is not connected to this processor.");
                    }

                    //forward(child, key, value);
                }
            }
            finally
            {
                this.RecordContext = previousContext;
                this.SetCurrentNode(previousNode);
            }
        }

        public override void Commit()
        {
            this.task.RequestCommit();
        }

        public override ICancellable Schedule(
            TimeSpan interval,
            PunctuationType type,
            IPunctuator callback)
        {
            var msgPrefix = ApiUtils.PrepareMillisCheckFailMsgPrefix(interval, "interval");
            return this.Schedule(ApiUtils.ValidateMillisecondDuration(interval, msgPrefix), type, callback);
        }

        public void SetCurrentNode(IProcessorNode<K, V> currentNode)
        {
            this.CurrentNode = currentNode;
        }
    }
}
