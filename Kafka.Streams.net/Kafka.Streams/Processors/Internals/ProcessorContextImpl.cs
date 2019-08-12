//using Kafka.Streams.State;
//using Kafka.Streams.Processor.Interfaces;
//using Kafka.Streams.State.Internals;
//using Kafka.Streams.Processor.Internals.Metrics;
//using Kafka.Streams.Errors;
//using System.Collections.Generic;
//using System;
//using Kafka.Streams.Internals.Kafka.Streams.Internals;

//namespace Kafka.Streams.Processor.Internals
//{
//    public class ProcessorContextImpl<K, V> : AbstractProcessorContext<K, V>, ISupplier
//    {
//        private StreamTask<K, V> task;
//        private IRecordCollector<K, V> collector;
//        private ToInternal toInternal = new ToInternal();
//        private static To SEND_TO_ALL = To.all();

//        public ProcessorContextImpl(TaskId id,
//                             StreamTask<K, V> task,
//                             StreamsConfig config,
//                             IRecordCollector<K, V> collector,
//                             ProcessorStateManager<K, V> stateMgr,
//                             StreamsMetricsImpl metrics,
//                             ThreadCache cache)
//            : base(id, config, metrics, stateMgr, cache)
//        {
//            this.task = task;
//            this.collector = collector;
//        }

//        public ProcessorStateManager<K, V> getStateMgr()
//        {
//            return (ProcessorStateManager<K, V>)stateManager;
//        }


//        public IRecordCollector<K, V> recordCollector()
//        {
//            return collector;
//        }

//        /**
//         * @throws StreamsException if an attempt is made to access this state store from an unknown node
//         */


//        public IStateStore getStateStore(string name)
//        {
//            if (currentNode() == null)
//            {
//                throw new StreamsException("Accessing from an unknown node");
//            }

//            IStateStore global = stateManager.getGlobalStore(name);
//            if (global != null)
//            {
//                if (global is ITimestampedKeyValueStore<K, V>)
//                {
//                    return new TimestampedKeyValueStoreReadOnlyDecorator<K, V>((ITimestampedKeyValueStore<K, V>)global);
//                }
//                else if (global is IKeyValueStore<K, V>)
//                {
//                    return new KeyValueStoreReadOnlyDecorator((IKeyValueStore<K, V>)global);
//                }
//                else if (global is ITimestampedWindowStore<K, V>)
//                {
//                    return new TimestampedWindowStoreReadOnlyDecorator((ITimestampedWindowStore)global);
//                }
//                else if (global is IWindowStore)
//                {
//                    return new WindowStoreReadOnlyDecorator((IWindowStore)global);
//                }
//                else if (global is ISessionStore)
//                {
//                    return new SessionStoreReadOnlyDecorator((ISessionStore)global);
//                }

//                return global;
//            }

//            if (!currentNode().stateStores.Contains(name))
//            {
//                throw new StreamsException("IProcessor " + currentNode().name + " has no access to IStateStore " + name +
//                    " as the store is not connected to the processor. If you.Add stores manually via '.AddStateStore()' " +
//                    "make sure to connect the.Added store to the processor by providing the processor name to " +
//                    "'.AddStateStore()' or connect them via '.connectProcessorAndStateStores()'. " +
//                    "DSL users need to provide the store name to '.process()', '.transform()', or '.transformValues()' " +
//                    "to connect the store to the corresponding operator. If you do not.Add stores manually, " +
//                    "please file a bug report at https://issues.apache.org/jira/projects/KAFKA.");
//            }

//            IStateStore store = stateManager.getStore(name);
//            if (store is ITimestampedKeyValueStore)
//            {
//                return new TimestampedKeyValueStoreReadWriteDecorator((ITimestampedKeyValueStore)store);
//            }
//            else if (store is IKeyValueStore)
//            {
//                return new KeyValueStoreReadWriteDecorator((IKeyValueStore)store);
//            }
//            else if (store is ITimestampedWindowStore)
//            {
//                return new TimestampedWindowStoreReadWriteDecorator((ITimestampedWindowStore)store);
//            }
//            else if (store is IWindowStore)
//            {
//                return new WindowStoreReadWriteDecorator((IWindowStore)store);
//            }
//            else if (store is ISessionStore)
//            {
//                return new SessionStoreReadWriteDecorator((ISessionStore)store);
//            }

//            return store;
//        }

//        public void forward(K key,
//                                   V value)
//        {
//            forward(key, value, SEND_TO_ALL);
//        }

//        public void forward(
//            K key,
//            V value,
//            To to)
//        {
//            ProcessorNode previousNode = currentNode();
//            ProcessorRecordContext previousContext = recordContext;

//            try
//            {

//                toInternal.update(to);
//                if (toInternal.hasTimestamp())
//                {
//                    recordContext = new ProcessorRecordContext(
//                        toInternal.timestamp(),
//                        recordContext.offset(),
//                        recordContext.partition(),
//                        recordContext.Topic,
//                        recordContext.headers());
//                }

//                string sendTo = toInternal.child();
//                if (sendTo == null)
//                {
//                    List<ProcessorNode<K, V>> children = (List<ProcessorNode<K, V>>)currentNode().children();
//                    foreach (ProcessorNode child in children)
//                    {
//                        forward(child, key, value);
//                    }
//                }
//                else
//                {

//                    ProcessorNode child = currentNode().getChild(sendTo);
//                    if (child == null)
//                    {
//                        throw new StreamsException("Unknown downstream node: " + sendTo
//                            + " either does not exist or is not connected to this processor.");
//                    }
//                    forward(child, key, value);
//                }
//            }
//            finally
//            {

//                recordContext = previousContext;
//                setCurrentNode(previousNode);
//            }
//        }


//        private void forward(
//            ProcessorNode child,
//            K key,
//            V value)
//        {
//            setCurrentNode(child);
//            child.process(key, value);
//        }


//        public void commit()
//        {
//            task.requestCommit();
//        }

//        public ICancellable schedule(
//            TimeSpan interval,
//            PunctuationType type,
//            Punctuator callback)
//        {
//            string msgPrefix = prepareMillisCheckFailMsgPrefix(interval, "interval");
//            return schedule(ApiUtils.validateMillisecondDuration(interval, msgPrefix), type, callback);
//        }

//        IRecordCollector recordCollector()
//        {
//            throw new NotImplementedException();
//        }
//    }
//}
