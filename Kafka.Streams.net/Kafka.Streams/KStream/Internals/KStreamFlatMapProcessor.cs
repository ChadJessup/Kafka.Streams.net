using Kafka.Streams.Processors;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamFlatMapProcessor<K, V> : AbstractProcessor<K, V>
    {
        public override void process(K key, V value)
        {
            //foreach (var newPair in mapper.apply(key, value))
            //{
            //    context.forward(newPair.key, newPair.value);
            //}
        }
    }
}