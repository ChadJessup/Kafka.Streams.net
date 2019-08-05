using Kafka.streams.state;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;

namespace Kafka.Streams.Processor.Internals
{
    public class ProcessorContextImpl : AbstractProcessorContext, RecordCollector.Supplier
    {
        private StreamTask task;
        private RecordCollector collector;
        private ToInternal toInternal = new ToInternal();
        private static To SEND_TO_ALL = To.all();

        ProcessorContextImpl(TaskId id,
                             StreamTask task,
                             StreamsConfig config,
                             RecordCollector collector,
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


        public RecordCollector recordCollector()
        {
            return collector;
        }

        /**
         * @throws StreamsException if an attempt is made to access this state store from an unknown node
         */


        public IStateStore getStateStore(string name)
        {
            if (currentNode() == null)
            {
                throw new StreamsException("Accessing from an unknown node");
            }

            IStateStore global = stateManager.getGlobalStore(name);
            if (global != null)
            {
                if (global is TimestampedKeyValueStore)
                {
                    return new TimestampedKeyValueStoreReadOnlyDecorator((TimestampedKeyValueStore)global);
                }
                else if (global is IKeyValueStore)
                {
                    return new KeyValueStoreReadOnlyDecorator((IKeyValueStore)global);
                }
                else if (global is TimestampedWindowStore)
                {
                    return new TimestampedWindowStoreReadOnlyDecorator((TimestampedWindowStore)global);
                }
                else if (global is WindowStore)
                {
                    return new WindowStoreReadOnlyDecorator((WindowStore)global);
                }
                else if (global is ISessionStore)
                {
                    return new SessionStoreReadOnlyDecorator((ISessionStore)global);
                }

                return global;
            }

            if (!currentNode().stateStores.contains(name))
            {
                throw new StreamsException("Processor " + currentNode().name() + " has no access to IStateStore " + name +
                    " as the store is not connected to the processor. If you.Add stores manually via '.AddStateStore()' " +
                    "make sure to connect the.Added store to the processor by providing the processor name to " +
                    "'.AddStateStore()' or connect them via '.connectProcessorAndStateStores()'. " +
                    "DSL users need to provide the store name to '.process()', '.transform()', or '.transformValues()' " +
                    "to connect the store to the corresponding operator. If you do not.Add stores manually, " +
                    "please file a bug report at https://issues.apache.org/jira/projects/KAFKA.");
            }

            IStateStore store = stateManager.getStore(name);
            if (store is TimestampedKeyValueStore)
            {
                return new TimestampedKeyValueStoreReadWriteDecorator((TimestampedKeyValueStore)store);
            }
            else if (store is IKeyValueStore)
            {
                return new KeyValueStoreReadWriteDecorator((IKeyValueStore)store);
            }
            else if (store is TimestampedWindowStore)
            {
                return new TimestampedWindowStoreReadWriteDecorator((TimestampedWindowStore)store);
            }
            else if (store is WindowStore)
            {
                return new WindowStoreReadWriteDecorator((WindowStore)store);
            }
            else if (store is ISessionStore)
            {
                return new SessionStoreReadWriteDecorator((ISessionStore)store);
            }

            return store;
        }



        public void forward(K key,
                                   V value)
        {
            forward(key, value, SEND_TO_ALL);
        }



        [System.Obsolete]
        public void forward(K key,
                                   V value,
                                   int childIndex)
        {
            forward(
                key,
                value,
                To.child(((List<ProcessorNode>)currentNode().children())[childIndex).name()));
        }



        [System.Obsolete]
        public void forward(K key,
                                   V value,
                                   string childName)
        {
            forward(key, value, To.child(childName));
        }



        public void forward(K key,
                                   V value,
                                   To to)
        {
            ProcessorNode previousNode = currentNode();
            ProcessorRecordContext previousContext = recordContext;

            try
            {

                toInternal.update(to);
                if (toInternal.hasTimestamp())
                {
                    recordContext = new ProcessorRecordContext(
                        toInternal.timestamp(),
                        recordContext.offset(),
                        recordContext.partition(),
                        recordContext.topic(),
                        recordContext.headers());
                }

                string sendTo = toInternal.child();
                if (sendTo == null)
                {
                    List<ProcessorNode<K, V>> children = (List<ProcessorNode<K, V>>)currentNode().children();
                    foreach (ProcessorNode child in children)
                    {
                        forward(child, key, value);
                    }
                }
                else
                {

                    ProcessorNode child = currentNode().getChild(sendTo);
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


        private void forward(ProcessorNode child,
                                    K key,
                                    V value)
        {
            setCurrentNode(child);
            child.process(key, value);
        }


        public void commit()
        {
            task.requestCommit();
        }


        [System.Obsolete]
        public ICancellable schedule(long intervalMs,
                                    PunctuationType type,
                                    Punctuator callback)
        {
            if (intervalMs < 1)
            {
                throw new System.ArgumentException("The minimum supported scheduling interval is 1 millisecond.");
            }
            return task.schedule(intervalMs, type, callback);
        }



        public ICancellable schedule(TimeSpan interval,
                                    PunctuationType type,
                                    Punctuator callback)
        {
            string msgPrefix = prepareMillisCheckFailMsgPrefix(interval, "interval");
            return schedule(ApiUtils.validateMillisecondDuration(interval, msgPrefix), type, callback);
        }

        private abstract static class StateStoreReadOnlyDecorator<T, K, V>
        : WrappedStateStore<T, K, V>
            where T : IStateStore
        {

            static string ERROR_MESSAGE = "Global store is read only";

            private StateStoreReadOnlyDecorator(T inner)
                : base(inner)
            {
            }


            public void flush()
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }


            public void init(IProcessorContext context,
                             IStateStore root)
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }


            public void close()
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }
        }

        private static class KeyValueStoreReadOnlyDecorator<K, V>
            : StateStoreReadOnlyDecorator<IKeyValueStore<K, V>, K, V>
            , IKeyValueStore<K, V>
        {

            private KeyValueStoreReadOnlyDecorator(IKeyValueStore<K, V> inner)
            : base(inner)
            {
            }


            public V get(K key)
            {
                return wrapped()[key);
            }


            public KeyValueIterator<K, V> range(K from,
                                                K to)
            {
                return wrapped().range(from, to);
            }


            public KeyValueIterator<K, V> all()
            {
                return wrapped().all();
            }


            public long approximateNumEntries()
            {
                return wrapped().approximateNumEntries();
            }


            public void put(K key,
                            V value)
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }


            public V putIfAbsent(K key,
                                 V value)
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }


            public void putAll(List entries)
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }


            public V delete(K key)
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }
        }

        private static class TimestampedKeyValueStoreReadOnlyDecorator<K, V>
            : KeyValueStoreReadOnlyDecorator<K, ValueAndTimestamp<V>>
            , TimestampedKeyValueStore<K, V>
        {

            private TimestampedKeyValueStoreReadOnlyDecorator(TimestampedKeyValueStore<K, V> inner)
                : base(inner)
            {
            }
        }

        private static class WindowStoreReadOnlyDecorator<K, V>
                : StateStoreReadOnlyDecorator<WindowStore<K, V>, K, V>
                , WindowStore<K, V>
        {

            private WindowStoreReadOnlyDecorator(WindowStore<K, V> inner)
        : base(inner)
            {
            }


            public void put(K key,
                            V value)
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }


            public void put(K key,
                            V value,
                            long windowStartTimestamp)
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }


            public V fetch(K key,
                           long time)
            {
                return wrapped().fetch(key, time);
            }


            [System.Obsolete]
            public WindowStoreIterator<V> fetch(K key,
                                                long timeFrom,
                                                long timeTo)
            {
                return wrapped().fetch(key, timeFrom, timeTo);
            }


            [System.Obsolete]
            public KeyValueIterator<Windowed<K>, V> fetch(K from,
                                                          K to,
                                                          long timeFrom,
                                                          long timeTo)
            {
                return wrapped().fetch(from, to, timeFrom, timeTo);
            }


            public KeyValueIterator<Windowed<K>, V> all()
            {
                return wrapped().all();
            }


            [System.Obsolete]
            public KeyValueIterator<Windowed<K>, V> fetchAll(long timeFrom,
                                                             long timeTo)
            {
                return wrapped().fetchAll(timeFrom, timeTo);
            }
        }

        private static class TimestampedWindowStoreReadOnlyDecorator<K, V>
            : WindowStoreReadOnlyDecorator<K, ValueAndTimestamp<V>>
            , TimestampedWindowStore<K, V>
        {

            private TimestampedWindowStoreReadOnlyDecorator(TimestampedWindowStore<K, V> inner)
        : base(inner)
            {
            }
        }

        private static class SessionStoreReadOnlyDecorator<K, AGG>
            : StateStoreReadOnlyDecorator<ISessionStore<K, AGG>, K, AGG>
            , ISessionStore<K, AGG>
        {

            private SessionStoreReadOnlyDecorator(ISessionStore<K, AGG> inner)
        : base(inner)
            {
            }


            public KeyValueIterator<Windowed<K>, AGG> findSessions(K key,
                                                                   long earliestSessionEndTime,
                                                                   long latestSessionStartTime)
            {
                return wrapped().findSessions(key, earliestSessionEndTime, latestSessionStartTime);
            }


            public KeyValueIterator<Windowed<K>, AGG> findSessions(K keyFrom,
                                                                   K keyTo,
                                                                   long earliestSessionEndTime,
                                                                   long latestSessionStartTime)
            {
                return wrapped().findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
            }


            public void Remove(Windowed sessionKey)
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }


            public void put(Windowed<K> sessionKey,
                            AGG aggregate)
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }


            public AGG fetchSession(K key, long startTime, long endTime)
            {
                return wrapped().fetchSession(key, startTime, endTime);
            }


            public KeyValueIterator<Windowed<K>, AGG> fetch(K key)
            {
                return wrapped().fetch(key);
            }


            public KeyValueIterator<Windowed<K>, AGG> fetch(K from,
                                                            K to)
            {
                return wrapped().fetch(from, to);
            }
        }

        private abstract static class StateStoreReadWriteDecorator<T, K, V>
            : WrappedStateStore<T, K, V>
        where T : IStateStore
        {

            static string ERROR_MESSAGE = "This method may only be called by Kafka Streams";

            private StateStoreReadWriteDecorator(T inner)
            {
                base(inner);
            }


            public void init(IProcessorContext context,
                             IStateStore root)
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }


            public void close()
            {
                throw new InvalidOperationException(ERROR_MESSAGE);
            }
        }

        static class KeyValueStoreReadWriteDecorator<K, V>
            : StateStoreReadWriteDecorator<IKeyValueStore<K, V>, K, V>
            , IKeyValueStore<K, V>
        {

            KeyValueStoreReadWriteDecorator(IKeyValueStore<K, V> inner)
        : base(inner)
            {
            }


            public V get(K key)
            {
                return wrapped()[key];
            }


            public KeyValueIterator<K, V> range(K from,
                                                K to)
            {
                return wrapped().range(from, to);
            }


            public KeyValueIterator<K, V> all()
            {
                return wrapped().all();
            }


            public long approximateNumEntries()
            {
                return wrapped().approximateNumEntries();
            }


            public void put(K key,
                            V value)
            {
                wrapped().Add(key, value);
            }


            public V putIfAbsent(K key,
                                 V value)
            {
                return wrapped().putIfAbsent(key, value);
            }


            public void putAll(List<KeyValue<K, V>> entries)
            {
                wrapped().putAll(entries);
            }


            public V delete(K key)
            {
                return wrapped().delete(key);
            }
        }

        static class TimestampedKeyValueStoreReadWriteDecorator<K, V>
            : KeyValueStoreReadWriteDecorator<K, ValueAndTimestamp<V>>
            , TimestampedKeyValueStore<K, V>
        {

            TimestampedKeyValueStoreReadWriteDecorator(TimestampedKeyValueStore<K, V> inner)
        : base(inner)
            {
            }
        }

        static class WindowStoreReadWriteDecorator<K, V>
            : StateStoreReadWriteDecorator<WindowStore<K, V>, K, V>
            , WindowStore<K, V>
        {

            WindowStoreReadWriteDecorator(WindowStore<K, V> inner)
        : base(inner)
            {
            }


            public void put(K key,
                            V value)
            {
                wrapped().Add(key, value);
            }


            public void put(K key,
                            V value,
                            long windowStartTimestamp)
            {
                wrapped().Add(key, value, windowStartTimestamp);
            }


            public V fetch(K key,
                           long time)
            {
                return wrapped().fetch(key, time);
            }



            public WindowStoreIterator<V> fetch(K key,
                                                long timeFrom,
                                                long timeTo)
            {
                return wrapped().fetch(key, timeFrom, timeTo);
            }



            public KeyValueIterator<Windowed<K>, V> fetch(K from,
                                                          K to,
                                                          long timeFrom,
                                                          long timeTo)
            {
                return wrapped().fetch(from, to, timeFrom, timeTo);
            }



            public KeyValueIterator<Windowed<K>, V> fetchAll(long timeFrom,
                                                             long timeTo)
            {
                return wrapped().fetchAll(timeFrom, timeTo);
            }


            public KeyValueIterator<Windowed<K>, V> all()
            {
                return wrapped().all();
            }
        }

        static class TimestampedWindowStoreReadWriteDecorator<K, V>
            : WindowStoreReadWriteDecorator<K, ValueAndTimestamp<V>>
            , TimestampedWindowStore<K, V>
        {

            TimestampedWindowStoreReadWriteDecorator(TimestampedWindowStore<K, V> inner)
                : base(inner)
            {
            }
        }

        static class SessionStoreReadWriteDecorator<K, AGG>
            : StateStoreReadWriteDecorator<ISessionStore<K, AGG>, K, AGG>
            , ISessionStore<K, AGG>
        {

            SessionStoreReadWriteDecorator(ISessionStore<K, AGG> inner)
            {
                base(inner);
            }


            public KeyValueIterator<Windowed<K>, AGG> findSessions(K key,
                                                                   long earliestSessionEndTime,
                                                                   long latestSessionStartTime)
            {
                return wrapped().findSessions(key, earliestSessionEndTime, latestSessionStartTime);
            }


            public KeyValueIterator<Windowed<K>, AGG> findSessions(K keyFrom,
                                                                   K keyTo,
                                                                   long earliestSessionEndTime,
                                                                   long latestSessionStartTime)
            {
                return wrapped().findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
            }


            public void Remove(Windowed<K> sessionKey)
            {
                wrapped().Remove(sessionKey);
            }


            public void put(Windowed<K> sessionKey,
                            AGG aggregate)
            {
                wrapped().Add(sessionKey, aggregate);
            }


            public AGG fetchSession(K key,
                                    long startTime,
                                    long endTime)
            {
                return wrapped().fetchSession(key, startTime, endTime);
            }


            public KeyValueIterator<Windowed<K>, AGG> fetch(K key)
            {
                return wrapped().fetch(key);
            }


            public KeyValueIterator<Windowed<K>, AGG> fetch(K from,
                                                            K to)
            {
                return wrapped().fetch(from, to);
            }
        }
    }

