
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatMapValues<K, V, VR> : IProcessorSupplier<K, V>
    {
        private readonly IValueMapperWithKey<K, V, IEnumerable<VR>> mapper;

        public KStreamFlatMapValues(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
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
