using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamMapValues<K, V, V1> : IProcessorSupplier<K, V>
    {
        private readonly ValueMapperWithKey<K, V, V1> mapper;

        public KStreamMapValues(ValueMapperWithKey<K, V, V1> mapper)
            => this.mapper = mapper;

        public IKeyValueProcessor<K, V> Get()
            => new KStreamMapProcessor<K, V>();

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
