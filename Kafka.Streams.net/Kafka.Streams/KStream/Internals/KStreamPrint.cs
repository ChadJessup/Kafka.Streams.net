using Kafka.Streams.Processors;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamPrint<K, V> : IProcessorSupplier<K, V>
    {
        private readonly Action<K, V> action;
        public KStreamPrint(Action<K, V> action)
        {
            this.action = action;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return null;// new KStreamPrintProcessor();
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}