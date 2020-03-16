using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.Processors;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamMap<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        private readonly IKeyValueMapper<K, V, KeyValue<K1, V1>> mapper;

        public KStreamMap(IKeyValueMapper<K, V, KeyValue<K1, V1>> mapper)
        {
            this.mapper = mapper;
        }

        public KStreamMap(Func<K, V, KeyValue<K1, V1>> mapper)
        {
            this.mapper = new KeyValueMapper<K, V, KeyValue<K1, V1>>(mapper);
        }

        public IKeyValueProcessor<K, V> get()
        {
            return new KStreamMapProcessor<K, V>();
        }
    }
}
