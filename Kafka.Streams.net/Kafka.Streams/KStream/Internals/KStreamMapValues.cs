using System;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.KStream.Mappers;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamMapValues<K, V, V1> : IProcessorSupplier<K, V>
    {
        private readonly IValueMapperWithKey<K, V, V1> mapper;
        //private IValueMapperWithKey<K, V1, V> swappedMapper;

        public KStreamMapValues(IValueMapperWithKey<K, V, V1> mapper)
            => this.mapper = mapper;

        //public KStreamMapValues(Func<K, V1, V> swappedMapper)
        //    => this.swappedMapper = new ValueMapperWithKey<K, V1, V>(swappedMapper);

        public KStreamMapValues(Func<K, V, V1> mapper)
            => this.mapper = new ValueMapperWithKey<K, V, V1>(mapper);

        public IKeyValueProcessor<K, V> get()
            => new KStreamMapProcessor<K, V>();

        //public static implicit operator KStreamMapValues<K, V1, V>(KStreamMapValues<K, V, V1> toSwap)
        //    => new KStreamMapValues<K, V1, V>(toSwap.swappedMapper);
    }
}
