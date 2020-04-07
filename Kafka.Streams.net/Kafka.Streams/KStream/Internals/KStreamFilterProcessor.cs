
using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFilterProcessor<K, V> : AbstractProcessor<K, V>
    {
        public override void Process(K key, V value)
        {
            //if (filterNot ^ predicate.test(key, value))
            //{
            //    context.Forward(key, value);
            //}
        }
    }
}
