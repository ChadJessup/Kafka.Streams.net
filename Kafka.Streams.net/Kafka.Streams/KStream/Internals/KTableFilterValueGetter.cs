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

        public void Init(IProcessorContext context, string storeName)
        {
            this.parentGetter.Init(context, storeName);
        }

        public IValueAndTimestamp<V> Get(K key)
        {
            return null; // computeValue(key, parentGetter[key]);
        }


        public void Close()
        {
            this.parentGetter.Close();
        }
    }
}
