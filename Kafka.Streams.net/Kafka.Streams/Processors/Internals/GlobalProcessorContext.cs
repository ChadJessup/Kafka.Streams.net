using Kafka.Streams.Configs;
using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
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
    public class GlobalProcessorContext : AbstractProcessorContext
    {
        public GlobalProcessorContext(
            StreamsConfig config,
            IStateManager stateManager,
            ThreadCache cache)
            : base(new TaskId(-1, -1), config, stateManager, cache)
        {
        }

        public GlobalProcessorContext(
            TaskId taskId,
            StreamsConfig config,
            IStateManager stateManager,
            ThreadCache cache)
            : base(taskId, config, stateManager, cache)
        {
        }
    }

    public class GlobalProcessorContext<K, V> : AbstractProcessorContext<K, V>
        where V : class
    {
        private readonly GlobalProcessorContext globalProcessContext;

        public GlobalProcessorContext(
            StreamsConfig config,
            IStateManager stateMgr,
            ThreadCache cache)
            : base(new TaskId(-1, -1), config, stateMgr, cache)
        {
            this.globalProcessContext = new GlobalProcessorContext(
                new TaskId(-1, -1), config, stateMgr, cache);
        }

        public override IStateStore GetStateStore(string name)
        {
            var store = StateManager.GetGlobalStore(name);

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

        public override void Forward<K, V>(K key, V value)
        {
            var previousNode = CurrentNode;
            try
            {
                foreach (var child in CurrentNode.Children)
                {
                    SetCurrentNode(child);
                    child.Process(key, value);
                }
            }
            finally
            {
                SetCurrentNode(previousNode);
            }
        }

        /**
         * No-op. This should only be called on GlobalStateStore#flush and there should be no child nodes
         */

        public override void Forward<K, V>(K key, V value, To to)
        {
            if (CurrentNode.Children.Any())
            {
                throw new Exception("This method should only be called on 'GlobalStateStore.flush' that should not have any children.");
            }
        }

        public override void Commit()
        {
            //no-op
        }

        /**
         * @throws InvalidOperationException on every invocation
         */
        [Obsolete]
        public ICancellable Schedule(long interval, PunctuationType type, IPunctuator callback)
        {
            throw new InvalidOperationException("this should not happen: schedule() not supported in global processor context.");
        }

        /**
         * @throws InvalidOperationException on every invocation
         */
        public override ICancellable Schedule(TimeSpan interval, PunctuationType type, IPunctuator callback)
        {
            throw new InvalidOperationException("this should not happen: schedule() not supported in global processor context.");
        }
    }
}
