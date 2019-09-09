using Kafka.Streams.Errors;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.State.Internals
{
    /**
     * A storage engine wrapper for utilities like logging, caching, and metering.
     */

    public abstract class WrappedStateStore : IStateStore
    {
        public static bool isTimestamped(IStateStore stateStore)
        {
            if (stateStore is ITimestampedBytesStore)
            {
                return true;
            }
            else if (stateStore is WrappedStateStore)
            {
                return isTimestamped(((WrappedStateStore)stateStore).GetWrappedStateStore());
            }
            else
            {
                return false;
            }
        }

        public abstract IStateStore GetWrappedStateStore();
        public abstract string name { get; }
        public abstract void close();
        public abstract void flush();
        public abstract void init<K, V>(IProcessorContext<K, V> context, IStateStore root);
        public abstract bool isOpen();
        public abstract bool persistent();
    }

    public abstract class WrappedStateStore<S, K, V> : WrappedStateStore, ICachedStateStore<K, V>
        where S : IStateStore
    {
        public S wrapped { get; }

        public WrappedStateStore(S wrapped)
        {
            this.wrapped = wrapped;
        }

        public override void init<K, V>(IProcessorContext<K, V> context, IStateStore root)
        {
            wrapped.init(context, root);
        }

        public bool setFlushListener(ICacheFlushListener<K, V> listener, bool sendOldValues)
        {
            if (wrapped is ICachedStateStore<K, V>)
            {
                return ((ICachedStateStore<K, V>)wrapped).setFlushListener(listener, sendOldValues);
            }
            return false;
        }

        public override string name => wrapped.name;

        public override IStateStore GetWrappedStateStore()
            => this.wrapped;

        public override bool persistent()
        {
            return wrapped.persistent();
        }

        public override bool isOpen()
        {
            return wrapped.isOpen();
        }

        protected void validateStoreOpen()
        {
            if (!wrapped.isOpen())
            {
                throw new InvalidStateStoreException("Store " + wrapped.name + " is currently closed.");
            }
        }

        public override void flush()
        {
            wrapped.flush();
        }

        public override void close()
        {
            wrapped.close();
        }
    }
}