using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.Processor.Internals
{
    public abstract class StateStoreReadOnlyDecorator<T, K, V>
    : WrappedStateStore<T, K, V>
        where T : IStateStore
    {

        static string ERROR_MESSAGE = "Global store is read only";

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
