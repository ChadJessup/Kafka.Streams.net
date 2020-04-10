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
using System.Linq;

namespace Kafka.Streams.Processors.Internals
{
    public class GlobalProcessorContext : AbstractProcessorContext
    {
        public GlobalProcessorContext(
            KafkaStreamsContext context,
            StreamsConfig config,
            IStateManager stateManager,
            ThreadCache cache)
            : base(
                  context,
                  new TaskId(-1, -1),
                  config,
                  stateManager,
                  cache)
        {
        }

        public GlobalProcessorContext(
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
        }
    }

    public class GlobalProcessorContext<K, V> : AbstractProcessorContext<K, V>
        where V : class
    {
        private readonly GlobalProcessorContext globalProcessContext;

        public GlobalProcessorContext(
            KafkaStreamsContext context,
            StreamsConfig config,
            IStateManager stateMgr,
            ThreadCache cache)
            : base(
                  context,
                  new TaskId(-1, -1),
                  config,
                  stateMgr,
                  cache)
        {
            this.globalProcessContext = new GlobalProcessorContext(
                context,
                new TaskId(-1, -1),
                config,
                stateMgr,
                cache);
        }

        public override IStateStore GetStateStore(string Name)
        {
            var store = this.StateManager.GetGlobalStore(Name);

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

        public override void Forward<K, V>(K key, V value)
        {
            var previousNode = this.CurrentNode;
            try
            {
                foreach (var child in this.CurrentNode.Children)
                {
                    this.SetCurrentNode(child);
                    child.Process(key, value);
                }
            }
            finally
            {
                this.SetCurrentNode(previousNode);
            }
        }

        /**
         * No-op. This should only be called on GlobalStateStore#Flush and there should be no child nodes
         */

        public override void Forward<K, V>(K key, V value, To to)
        {
            if (this.CurrentNode.Children.Any())
            {
                throw new Exception("This method should only be called on 'GlobalStateStore.Flush' that should not have any children.");
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
