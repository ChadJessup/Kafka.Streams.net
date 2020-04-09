using System;
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
        public abstract string Name { get; }
        public abstract void Close();
        public abstract void Flush();
        public abstract void Init(IProcessorContext context, IStateStore root);
        public abstract bool IsOpen();
        public abstract bool Persistent();

        public abstract bool IsPresent();
    }

    public abstract class WrappedStateStore<K, V> : WrappedStateStore
    {
        public abstract bool SetFlushListener(FlushListener<K, V> listener, bool sendOldValues);
    }

    public abstract class WrappedStateStore<S, K, V> : WrappedStateStore<K, V>, ICachedStateStore
        where S : IStateStore
    {
        public KafkaStreamsContext Context { get; }
        public S Wrapped { get; }

        public WrappedStateStore(KafkaStreamsContext context, S wrapped)
        {
            this.Context = context;
            this.Wrapped = wrapped;
        }

        public override void Init(IProcessorContext context, IStateStore root) => Wrapped.Init(context, root);

        public override string Name => Wrapped.Name;

        public override IStateStore GetWrappedStateStore() => this.Wrapped;
        public override bool Persistent() => Wrapped.Persistent();
        public override bool IsOpen() => Wrapped.IsOpen();
        public override bool SetFlushListener(FlushListener<K, V> listener, bool sendOldValues)
        {
            if (Wrapped is ICachedStateStore)
            {
                return ((ICachedStateStore<K, V>)Wrapped).SetFlushListener(listener, sendOldValues);
            }

            return false;
        }

        protected void ValidateStoreOpen()
        {
            if (!Wrapped.IsOpen())
            {
                throw new InvalidStateStoreException("Store " + Wrapped.Name + " is currently closed.");
            }
        }

        public override bool IsPresent() => Wrapped.IsPresent();
        public override void Flush() => Wrapped.Flush();
        public override void Close() => Wrapped.Close();
    }
}
