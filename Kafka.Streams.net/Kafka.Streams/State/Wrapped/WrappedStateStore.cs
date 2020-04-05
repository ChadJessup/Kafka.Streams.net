using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.State.Internals
{
    /**
     * A storage engine wrapper for utilities like logging, caching, and metering.
     */
    public abstract class WrappedStateStore : IStateStore, ICachedStateStore
    {
        public static bool IsTimestamped(IStateStore stateStore)
        {
            if (stateStore is ITimestampedBytesStore)
            {
                return true;
            }
            else if (stateStore is WrappedStateStore)
            {
                return IsTimestamped(((WrappedStateStore)stateStore).GetWrappedStateStore());
            }
            else
            {
                return false;
            }
        }

        public abstract IStateStore GetWrappedStateStore();
        public abstract string name { get; }
        public abstract void Close();
        public abstract void Flush();
        public abstract void Init(IProcessorContext context, IStateStore root);
        public abstract bool IsOpen();
        public abstract bool Persistent();

        public bool IsPresent()
        {
            throw new System.NotImplementedException();
        }

        public abstract bool SetFlushListener<K, V>(ICacheFlushListener<K, V> listener, bool sendOldValues);
    }

    public abstract class WrappedStateStore<S> : WrappedStateStore
        where S : IStateStore
    {
        public S wrapped { get; }

        public WrappedStateStore(S wrapped)
        {
            this.wrapped = wrapped;
        }

        public override void Init(IProcessorContext context, IStateStore root)
        {
            wrapped.Init(context, root);
        }

        public override bool SetFlushListener<K, V>(ICacheFlushListener<K, V> listener, bool sendOldValues)
        {
            if (wrapped is ICachedStateStore)
            {
                return ((ICachedStateStore)wrapped).SetFlushListener(listener, sendOldValues);
            }

            return false;
        }

        public override string name => wrapped.name;

        public override IStateStore GetWrappedStateStore()
            => this.wrapped;

        public override bool Persistent()
        {
            return wrapped.Persistent();
        }

        public override bool IsOpen()
        {
            return wrapped.IsOpen();
        }

        protected void ValidateStoreOpen()
        {
            if (!wrapped.IsOpen())
            {
                throw new InvalidStateStoreException("Store " + wrapped.name + " is currently closed.");
            }
        }

        public override void Flush()
        {
            wrapped.Flush();
        }

        public override void Close()
        {
            wrapped.Close();
        }
    }
}