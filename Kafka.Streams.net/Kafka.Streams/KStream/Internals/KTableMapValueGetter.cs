using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMapValueGetter<K, V> : IKTableValueGetter<K, V>
    {
        private readonly IKTableValueGetter<K, V> parentGetter;
        private IProcessorContext context;
        private readonly KeyValueMapper<K, V, V> mapper;

        public KTableMapValueGetter(IKTableValueGetter<K, V> parentGetter)
        {
            this.parentGetter = parentGetter;
        }

        public void Init(IProcessorContext context, string storeName)
        {
            this.context = context;
            this.parentGetter.Init(context, storeName);
        }

        public IValueAndTimestamp<V>? Get(K key)
        {
            var valueAndTimestamp = this.parentGetter.Get(key);

            var mapped = this.mapper(key, valueAndTimestamp.Value);

            var timeStamp = valueAndTimestamp == null
                ? this.context.Timestamp
                : valueAndTimestamp.Timestamp;

            return ValueAndTimestamp.Make(
                mapped,
                timeStamp);
        }

        public void Close()
        {
            this.parentGetter.Close();
        }
    }
}
