using Kafka.Streams.KStream.Interfaces;
using System;

namespace Kafka.Streams.KStream.Mappers
{
    public class ValueMapperWithKey<K, V, VR> : IValueMapperWithKey<K, V, VR>
    {
        private readonly Func<K, V, VR> mapper;

        public ValueMapperWithKey(Func<K, V, VR> mapper)
            => this.mapper = mapper;

        public VR Apply(K readOnlyKey, V value)
            => this.mapper(readOnlyKey, value);
    }
}
