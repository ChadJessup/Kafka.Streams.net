using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.Processor.Internals
{
    public abstract class StateStoreReadWriteDecorator<T, K, V>
        : WrappedStateStore<T, K, V>
    where T : IStateStore
    {

        static string ERROR_MESSAGE = "This method may only be called by Kafka Streams";

        private StateStoreReadWriteDecorator(T inner)
        {
            base(inner);
        }


        public void init(IProcessorContext<K, V> context,
                         IStateStore root)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }


        public void close()
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }
    }
}
