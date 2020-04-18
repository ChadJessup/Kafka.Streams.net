using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatMapValues<K, V, VR> : IProcessorSupplier<K, V>
    {
        private readonly ValueMapperWithKey<K, V, IEnumerable<VR>> mapper;

        public KStreamFlatMapValues(ValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
        {
            this.mapper = mapper;
        }

        public IKeyValueProcessor<K, V> Get()
        {
            return new KStreamFlatMapValuesProcessor<K, V, VR>(this.mapper);
        }

        IKeyValueProcessor IProcessorSupplier.Get()
            => this.Get();
    }
}
