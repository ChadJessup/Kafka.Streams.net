using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatMapValuesProcessor<K, V, V1> : AbstractProcessor<K, V>
        where V1 : IEnumerable<V1>
    {
        private IValueMapperWithKey<K, V, IEnumerable<V1>> mapper;

        public KStreamFlatMapValuesProcessor(IValueMapperWithKey<K, V, IEnumerable<V1>> mapper)
            => this.mapper = mapper;

        public override void process(K key, V value)
        {
            var newValues = this.mapper.apply(key, value);
            foreach (V1 newValue in newValues)
            {
                context.forward(key, newValue);
            }
        }
    }
}
