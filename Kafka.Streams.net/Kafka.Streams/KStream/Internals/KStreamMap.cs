using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamMap<K, V, K1, V1> : IProcessorSupplier<K, V>
    {
        private readonly KeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;

        public KStreamMap(KeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper)
        {
            this.mapper = mapper;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return new KStreamMapProcessor<K, V>();
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
