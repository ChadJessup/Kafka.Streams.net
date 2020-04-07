
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamMapProcessor<K, V> : AbstractProcessor<K, V>
    {
        public override void Process(K key, V value)
        {
            // KeyValuePair<K1, V1> newPair = mapper.apply(key, value);
            // context.Forward(newPair.key, newPair.value);
        }
    }
}
