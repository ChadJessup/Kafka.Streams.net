using Kafka.Streams.Errors;
using Kafka.Streams.Processor.Interfaces;

namespace Kafka.Streams.State.Internals
{

    /**
     * A storage engine wrapper for utilities like logging, caching, and metering.
     */
    public abstract class WrappedStateStore<S, K, V> : IStateStore, CachedStateStore<K, V>
        where S : IStateStore
    {

        public static bool isTimestamped(IStateStore stateStore)
        {
            if (stateStore is TimestampedBytesStore)
            {
                return true;
            } else if (stateStore is WrappedStateStore)
            {
                return isTimestamped(((WrappedStateStore)stateStore).wrapped());
            } else
            {
                return false;
            }
        }

        private S wrapped;

        public WrappedStateStore(S wrapped)
        {
            this.wrapped = wrapped;
        }

        public override void init(IProcessorContext context, IStateStore root)
        {
            wrapped.init(context, root);
        }

        public override bool setFlushListener(CacheFlushListener<K, V> listener, bool sendOldValues)
        {
            if (wrapped is CachedStateStore)
            {
                return ((CachedStateStore<K, V>)wrapped).setFlushListener(listener, sendOldValues);
            }
            return false;
        }

        public override string name()
        {
            return wrapped.name();
        }

        public override bool persistent()
        {
            return wrapped.persistent();
        }

        public override bool isOpen()
        {
            return wrapped.isOpen();
        }

        void validateStoreOpen()
        {
            if (!wrapped.isOpen())
            {
                throw new InvalidStateStoreException("Store " + wrapped.name() + " is currently closed.");
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

        public S wrapped()
        {
            return wrapped;
        }
    }
}