using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatMap<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        private readonly IKeyValueMapper<K, V, IEnumerable<KeyValue<K1, V1>>> mapper;

        public KStreamFlatMap(
            IKeyValueMapper<K, V, IEnumerable<KeyValue<K1, V1>>> mapper)
        {
            this.mapper = mapper;
        }


        public IKeyValueProcessor<K, V> get()
        {
            return new KStreamFlatMapProcessor<K, V>();
        }
    }
}