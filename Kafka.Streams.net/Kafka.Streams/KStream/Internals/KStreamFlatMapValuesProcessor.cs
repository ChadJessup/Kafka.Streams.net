using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatMapValuesProcessor<K, V, VR> : AbstractProcessor<K, V>
    {
        private readonly IValueMapperWithKey<K, V, IEnumerable<VR>> mapper;

        public KStreamFlatMapValuesProcessor(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
            => this.mapper = mapper;

        public override void Process(K key, V value)
        {
            var newValues = this.mapper.Apply(key, value);
            foreach (VR newValue in newValues)
            {
                context.Forward(key, newValue);
            }
        }
    }
}
