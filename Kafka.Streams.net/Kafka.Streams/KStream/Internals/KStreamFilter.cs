using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFilter<K, V> : IProcessorSupplier<K, V>
    {
        private readonly FilterPredicate<K, V> predicate;
        private readonly bool filterNot;

        public KStreamFilter(FilterPredicate<K, V> predicate, bool filterNot)
        {
            this.predicate = predicate;
            this.FilterNot = filterNot;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return new KStreamFilterProcessor<K, V>();
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
