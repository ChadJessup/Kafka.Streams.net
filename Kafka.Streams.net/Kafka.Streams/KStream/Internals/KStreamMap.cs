using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.Processors;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamMap<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        private readonly IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;

        public KStreamMap(IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper)
        {
            this.mapper = mapper;
        }

        public KStreamMap(Func<K, V, KeyValuePair<K1, V1>> mapper)
        {
            this.mapper = new KeyValueMapper<K, V, KeyValuePair<K1, V1>>(mapper);
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return new KStreamMapProcessor<K, V>();
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
