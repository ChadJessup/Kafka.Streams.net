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
using Kafka.Streams.State.Window;
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
    {
        private readonly StreamTask task;
        private readonly IRecordCollector collector;
        private readonly ToInternal toInternal = new ToInternal();

        public ProcessorContext(
            TaskId id,
            StreamTask task,
            StreamsConfig config,
            IRecordCollector collector,
            ProcessorStateManager stateMgr,
            ThreadCache cache)
            : base(id, config, stateMgr, cache)
        {
            this.task = task;
            this.collector = collector;
        }

        public ProcessorStateManager getStateMgr()
        {
            return (ProcessorStateManager)stateManager;
        }

        public IRecordCollector recordCollector()
        {
            return collector;
        }

        /**
         * @throws StreamsException if an attempt is made to access this state store from an unknown node
         */
        public override IStateStore getStateStore(string name)
        {
            if (GetCurrentNode() == null)
            {
                throw new StreamsException("Accessing from an unknown node");
            }

            IStateStore? global = stateManager.GetGlobalStore(name);
            if (global != null)
            {
                if (global is ITimestampedKeyValueStore<K, V>)
                {
                    return new TimestampedKeyValueStoreReadOnlyDecorator<K, V>((ITimestampedKeyValueStore<K, V>)global);
                }
                else if (global is IKeyValueStore<K, V>)
                {
                    return new KeyValueStoreReadOnlyDecorator<K, V>((IKeyValueStore<K, V>)global);
                }
                else if (global is ITimestampedWindowStore<K, V>)
                {
                    return new TimestampedWindowStoreReadOnlyDecorator<K, V>((ITimestampedWindowStore<K, V>)global);
                }
                else if (global is IWindowStore<K, V>)
                {
                    return new WindowStoreReadOnlyDecorator<K, V>((IWindowStore<K, V>)global);
                }
                else if (global is ISessionStore<K, V>)
                {
                    return new SessionStoreReadOnlyDecorator<K, V>((ISessionStore<K, V>)global);
                }

                return global;
            }

            if (!GetCurrentNode().stateStores.Contains(name))
            {
                throw new StreamsException("IProcessor " + GetCurrentNode().Name + " has no access to IStateStore " + name +
                    " as the store is not connected to the processor. If you.Add stores manually via '.AddStateStore()' " +
                    "make sure to connect the.Added store to the processor by providing the processor name to " +
                    "'.AddStateStore()' or connect them via '.connectProcessorAndStateStores()'. " +
                    "DSL users need to provide the store name to '.process()', '.transform()', or '.transformValues()' " +
                    "to connect the store to the corresponding operator. If you do not.Add stores manually, " +
                    "please file a bug report at https://issues.apache.org/jira/projects/KAFKA.");
            }

            IStateStore? store = stateManager.GetStore(name);

            if (store is ITimestampedKeyValueStore<K, V>)
            {
                return new TimestampedKeyValueStoreReadWriteDecorator<K, V>((ITimestampedKeyValueStore<K, V>)store);
            }
            else if (store is IKeyValueStore<K, V>)
            {
                return new KeyValueStoreReadWriteDecorator<K, V>((IKeyValueStore<K, V>)store);
            }
            else if (store is ITimestampedWindowStore<K, V>)
            {
                return new TimestampedWindowStoreReadWriteDecorator<K, V>((ITimestampedWindowStore<K, V>)store);
            }
            else if (store is IWindowStore<K, V>)
            {
                return new WindowStoreReadWriteDecorator<K, V>((IWindowStore<K, V>)store);
            }
            else if (store is ISessionStore<K, V>)
            {
                return new SessionStoreReadWriteDecorator<K, V>((ISessionStore<K, V>)store);
            }

            return store;
        }

        public override void forward<K1, V1>(K1 key, V1 value)
        {
            forward(key, value, ProcessorContext.SEND_TO_ALL);
        }

        public override void forward<K1, V1>(K1 key, V1 value, To to)
        {
            var previousNode = GetCurrentNode();
            ProcessorRecordContext previousContext = recordContext;

            try
            {
                toInternal.Update(to);
                if (toInternal.hasTimestamp())
                {
                    recordContext = new ProcessorRecordContext(
                        toInternal.Timestamp,
                        recordContext.offset,
                        recordContext.partition,
                        recordContext.Topic,
                        recordContext.headers);
                }

                var sendTo = toInternal.child();
                if (sendTo == null)
                {
                    var children = new List<IProcessorNode<K, V>>(GetCurrentNode().children.Select(c => (IProcessorNode<K, V>)c));
                    foreach (var child in children)
                    {
                        //forward(child, key, value);
                    }
                }
                else
                {
                    var child = GetCurrentNode().GetChild(sendTo);
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
                recordContext = previousContext;
                SetCurrentNode(previousNode);
            }
        }

        public override void commit()
        {
            task.RequestCommit();
        }

        public ICancellable schedule(
            TimeSpan interval,
            PunctuationType type,
            IPunctuator callback)
        {
            var msgPrefix = ApiUtils.prepareMillisCheckFailMsgPrefix(interval, "interval");
            return schedule(ApiUtils.validateMillisecondDuration(interval, msgPrefix), type, callback);
        }

        public void SetCurrentNode(IProcessorNode<K, V> currentNode)
        {
            this.currentNode = currentNode;
        }
    }
}
