using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatMapValuesProcessor<K, V, VR> : AbstractProcessor<K, V>
    {
        private readonly ValueMapperWithKey<K, V, IEnumerable<VR>> mapper;

        public KStreamFlatMapValuesProcessor(ValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
            => this.mapper = mapper;

        public override void Process(K key, V value)
        {
            var newValues = this.mapper?.Invoke(key, value);
            foreach (VR newValue in newValues ?? Enumerable.Empty<VR>())
            {
                this.Context.Forward(key, newValue);
            }
        }
    }
}
