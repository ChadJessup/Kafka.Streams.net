
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamPeek<K, V> : IProcessorSupplier<K, V>
    {
        private readonly bool forwardDownStream;
        private readonly Action<K, V> action;

        public KStreamPeek(Action<K, V> action, bool forwardDownStream)
        {
            this.action = action;
            this.forwardDownStream = forwardDownStream;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return null;// new KStreamPeekProcessor();
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
