using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public abstract class StateStoreReadWriteDecorator<S, K, V> : WrappedStateStore<S, K, V>
        where S : IStateStore
    {
        protected const string ERROR_MESSAGE = "This method may only be called by Kafka Streams";

        public StateStoreReadWriteDecorator(KafkaStreamsContext context, S inner)
            : base(context, inner)
        {
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
