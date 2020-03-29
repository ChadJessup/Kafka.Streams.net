using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public abstract class StateStoreReadOnlyDecorator<T> : WrappedStateStore<T>
        where T : IStateStore
    {
        public static string ERROR_MESSAGE = "Global store is read only";

        public StateStoreReadOnlyDecorator(T inner)
            : base(inner)
        {
        }

        public override void Flush()
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public override void Init(IProcessorContext context, IStateStore root)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public override void Close()
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }
    }
}
