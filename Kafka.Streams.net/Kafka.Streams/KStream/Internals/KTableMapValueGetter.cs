using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMapValueGetter<K, V, K1, V1> : IKTableValueGetter<K, KeyValuePair<K1, V1>>
    {
        private readonly IKTableValueGetter<K, V> parentGetter;
        private IProcessorContext context;
        private readonly IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;

        public KTableMapValueGetter(IKTableValueGetter<K, V> parentGetter)
        {
            this.parentGetter = parentGetter;
        }

        public void init(IProcessorContext context, string storeName)
        {
            this.context = context;
            this.parentGetter.init(context, storeName);
        }

        public ValueAndTimestamp<KeyValuePair<K1, V1>> get(K key)
        {
            var valueAndTimestamp = parentGetter.get(key);

            var mapped = mapper.Apply(key, valueAndTimestamp.value);

            var timeStamp = valueAndTimestamp == null
                ? context.timestamp
                : valueAndTimestamp.timestamp;

            return ValueAndTimestamp<KeyValuePair<K1, V1>>.make(
                mapped,
                timeStamp);
        }

        public void close()
        {
            parentGetter.close();
        }
    }
}
