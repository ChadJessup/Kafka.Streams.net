using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatMap<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        private readonly KeyValueMapper<K, V, IEnumerable<KeyValuePair<K1, V1>>> mapper;

        public KStreamFlatMap(
            KeyValueMapper<K, V, IEnumerable<KeyValuePair<K1, V1>>> mapper)
        {
            this.mapper = mapper;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return new KStreamFlatMapProcessor<K, V>();
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}