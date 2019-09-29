using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableFilterValueGetter<K, V> : IKTableValueGetter<K, V>
    {
        private readonly IKTableValueGetter<K, V> parentGetter;

        public KTableFilterValueGetter(IKTableValueGetter<K, V> parentGetter)
        {
            this.parentGetter = parentGetter;
        }

        public void init(IProcessorContext context, string storeName)
        {
            parentGetter.init(context, storeName);
        }

        public ValueAndTimestamp<V> get(K key)
        {
            return null; // computeValue(key, parentGetter[key]);
        }


        public void close()
        {
            parentGetter.close();
        }
    }
}
