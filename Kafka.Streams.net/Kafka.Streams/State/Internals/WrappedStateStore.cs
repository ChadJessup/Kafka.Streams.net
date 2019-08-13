using Kafka.Streams.Errors;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.State.Internals
{
    /**
     * A storage engine wrapper for utilities like logging, caching, and metering.
     */
    public abstract class WrappedStateStore<S, K, V> : IStateStore, CachedStateStore<K, V>
        where S : IStateStore
    {
        public S wrapped { get; }

        public static bool isTimestamped(IStateStore stateStore)
        {
            //if (stateStore is ITimestampedBytesStore)
            {
              //  return true;
            }
            //else if (stateStore is WrappedStateStore)
            //{
            //    return isTimestamped(((WrappedStateStore)stateStore).wrapped);
            //}
            //else
            {
                return false;
            }
        }

        public WrappedStateStore(S wrapped)
        {
            this.wrapped = wrapped;
        }

        public void init<K, V>(
            IProcessorContext<K, V> context,
            IStateStore root)
        {
            wrapped.init(context, root);
        }

        public bool setFlushListener(ICacheFlushListener<K, V> listener, bool sendOldValues)
        {
            //if (wrapped is CachedStateStore)
            //{
            //    return ((CachedStateStore<K, V>)wrapped).setFlushListener(listener, sendOldValues);
            //}
            return false;
        }

        public string name => wrapped.name;

        public bool persistent()
        {
            return wrapped.persistent();
        }

        public bool isOpen()
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

        public void flush()
        {
            wrapped.flush();
        }

        public void close()
        {
            wrapped.close();
        }
    }
}