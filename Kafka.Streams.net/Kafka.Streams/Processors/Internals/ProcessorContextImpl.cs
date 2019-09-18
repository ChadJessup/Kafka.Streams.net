using Kafka.Streams.State;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Processor.Internals.Metrics;
using Kafka.Streams.Errors;
using System.Collections.Generic;
using System;
using Kafka.Streams.Internals;
using Kafka.Streams.State.Interfaces;
using System.Linq;
using Kafka.Streams.Interfaces;

namespace Kafka.Streams.Processor.Internals
{
    public class ProcessorContextImpl<K, V> : AbstractProcessorContext<K, V>, ISupplier
    {
        private readonly StreamTask task;
        private readonly IRecordCollector collector;
        private readonly ToInternal toInternal = new ToInternal();
        private static readonly To SEND_TO_ALL = To.all();

        public ProcessorContextImpl(
            TaskId id,
            StreamTask task,
            StreamsConfig config,
            IRecordCollector collector,
            ProcessorStateManager stateMgr,
            StreamsMetricsImpl metrics,
            ThreadCache cache)
            : base(id, config, metrics, stateMgr, cache)
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
            if (currentNode == null)
            {
                throw new StreamsException("Accessing from an unknown node");
            }

            IStateStore? global = stateManager.getGlobalStore(name);
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

            if (!currentNode.stateStores.Contains(name))
            {
                throw new StreamsException("IProcessor " + currentNode.name + " has no access to IStateStore " + name +
                    " as the store is not connected to the processor. If you.Add stores manually via '.AddStateStore()' " +
                    "make sure to connect the.Added store to the processor by providing the processor name to " +
                    "'.AddStateStore()' or connect them via '.connectProcessorAndStateStores()'. " +
                    "DSL users need to provide the store name to '.process()', '.transform()', or '.transformValues()' " +
                    "to connect the store to the corresponding operator. If you do not.Add stores manually, " +
                    "please file a bug report at https://issues.apache.org/jira/projects/KAFKA.");
            }

            IStateStore? store = stateManager.getStore(name);

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
            forward(key, value, SEND_TO_ALL);
        }

        public override void forward<K1, V1>(
            K1 key,
            V1 value,
            To to)
        {
            ProcessorNode<K, V> previousNode = currentNode;
            ProcessorRecordContext previousContext = recordContext;

            try
            {
                toInternal.update(to);
                if (toInternal.hasTimestamp())
                {
                    recordContext = new ProcessorRecordContext(
                        toInternal.timestamp,
                        recordContext.offset,
                        recordContext.partition,
                        recordContext.Topic,
                        recordContext.headers);
                }

                string sendTo = toInternal.child();
                if (sendTo == null)
                {
                    List<ProcessorNode<K, V>> children = new List<ProcessorNode<K, V>>(currentNode.children.Select(c => (ProcessorNode<K, V>)c));
                    foreach (var child in children)
                    {
                        forward(child, key, value);
                    }
                }
                else
                {
                    ProcessorNode<K, V> child = currentNode.getChild(sendTo);
                    if (child == null)
                    {
                        throw new StreamsException("Unknown downstream node: " + sendTo
                            + " either does not exist or is not connected to this processor.");
                    }

                    forward(child, key, value);
                }
            }
            finally
            {
                recordContext = previousContext;
                setCurrentNode(previousNode);
            }
        }

        private void forward<K1, V1>(
            ProcessorNode<K, V> child,
            K key,
            V value)
        {
            setCurrentNode(child);
            child.process(key, value);
        }

        public override void commit()
        {
            task.requestCommit();
        }

        public ICancellable schedule(
            TimeSpan interval,
            PunctuationType type,
            Punctuator callback)
        {
            string msgPrefix = ApiUtils.prepareMillisCheckFailMsgPrefix(interval, "interval");
            return schedule(ApiUtils.validateMillisecondDuration(interval, msgPrefix), type, callback);
        }

        public override void setCurrentNode(ProcessorNode<K, V> currentNode)
        {
            this.currentNode = currentNode;
        }
    }
}
