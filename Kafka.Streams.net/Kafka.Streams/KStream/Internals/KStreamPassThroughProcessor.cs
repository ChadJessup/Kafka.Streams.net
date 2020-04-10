
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamPassThroughProcessor<K, V> : AbstractProcessor<K, V>
    {
        public override void Process(K key, V value)
        {
            this.Context.Forward(key, value);
        }
    }
}
