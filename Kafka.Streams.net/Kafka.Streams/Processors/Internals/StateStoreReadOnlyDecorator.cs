using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.Processor.Internals
{
    public abstract class StateStoreReadOnlyDecorator<T> : WrappedStateStore<T>
        where T : IStateStore
    {
        public static string ERROR_MESSAGE = "Global store is read only";

        public StateStoreReadOnlyDecorator(T inner)
            : base(inner)
        {
        }

        public override void flush()
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public override void init<K, V>(IProcessorContext<K, V> context, IStateStore root)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public override void close()
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }
    }
}
