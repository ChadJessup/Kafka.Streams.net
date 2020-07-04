using Kafka.Streams.KStream.Interfaces;
using System;

namespace Kafka.Streams.KStream
{
    public class WrappedAggregator<K, V, VA> : IAggregator2<K, V, VA>
    {
        private readonly Aggregator<K, V, VA> aggregator;

        public WrappedAggregator(Aggregator<K, V, VA> aggregator)
            => this.aggregator = aggregator;

        public VA Apply(K key, V value, VA aggregate)
            => this.aggregator(key, value, aggregate);
    }
}
