using System;

namespace Kafka.Streams.KStream.Mappers
{
    public class KeyValueMapper<K, V, VR> : IKeyValueMapper<K, V, VR>
    {
        private readonly Func<K, V, VR> mapper;

        public KeyValueMapper(Func<K, V, VR> mapper)
            => this.mapper = mapper;

        public VR apply(K key, V value)
            => this.mapper(key, value);
    }
}
