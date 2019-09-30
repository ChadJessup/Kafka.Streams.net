using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public abstract class StateStoreReadWriteDecorator<T> : WrappedStateStore<T>
    where T : IStateStore
    {

        static readonly string ERROR_MESSAGE = "This method may only be called by Kafka Streams";

        public StateStoreReadWriteDecorator(T inner)
            : base(inner)
        {
        }

        public override void init(IProcessorContext context, IStateStore root)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public override void close()
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }
    }
}
