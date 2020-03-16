using Kafka.Streams.KStream.Interfaces;
using System;

namespace Kafka.Streams.KStream
{
    public class Aggregator<K, V, VA> : IAggregator<K, V, VA>
    {
        private readonly Func<K, V, VA> aggregator;

        public Aggregator(Func<K, V, VA> aggregator)
            => this.aggregator = aggregator;

        public VA apply(K key, V value, VA aggregate)
            => this.aggregator(key, value);
    }
}
