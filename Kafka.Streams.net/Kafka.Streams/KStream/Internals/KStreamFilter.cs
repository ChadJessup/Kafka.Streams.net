using Kafka.Streams.Processors;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFilter<K, V> : IProcessorSupplier<K, V>
    {
        private readonly Func<K, V, bool> predicate;
        private readonly bool filterNot;

        public KStreamFilter(Func<K, V, bool> predicate, bool filterNot)
        {
            this.predicate = predicate;
            this.filterNot = filterNot;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return new KStreamFilterProcessor<K, V>();
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}