using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public abstract class StateStoreReadOnlyDecorator<S, K, V> : WrappedStateStore<S, K, V>
        where S : IStateStore
    {
        public const string ERROR_MESSAGE = "Global store is read only";

        public StateStoreReadOnlyDecorator(KafkaStreamsContext context, S inner)
            : base(context, inner)
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
