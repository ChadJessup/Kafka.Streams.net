using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatMapValuesProcessor<K, V, VR> : AbstractProcessor<K, V>
    {
        private IValueMapperWithKey<K, V, IEnumerable<VR>> mapper;

        public KStreamFlatMapValuesProcessor(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
            => this.mapper = mapper;

        public override void process(K key, V value)
        {
            var newValues = this.mapper.apply(key, value);
            foreach (VR newValue in newValues)
            {
                context.forward(key, newValue);
            }
        }
    }
}
