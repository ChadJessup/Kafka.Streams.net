using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMapValueGetter<K, V, K1, V1> : IKTableValueGetter<K, KeyValue<K1, V1>>
    {
        private readonly IKTableValueGetter<K, KeyValue<K1, V1>> parentGetter;
        private IProcessorContext<K, KeyValue<K1, V1>> context;
        private readonly IKeyValueMapper<K, V, KeyValue<K1, V1>> mapper;

        public KTableMapValueGetter(IKTableValueGetter<K, KeyValue<K1, V1>> parentGetter)
        {
            this.parentGetter = parentGetter;
        }

        public void init(IProcessorContext<K, KeyValue<K1, V1>> context, string storeName)
        {
            this.context = context;
            this.parentGetter.init(context, storeName);
        }

        public ValueAndTimestamp<KeyValue<K1, V1>> get(K key)
        {
            var valueAndTimestamp = parentGetter.get(key);

            var mapped = mapper.apply(key, valueAndTimestamp.value);

            var timeStamp = valueAndTimestamp == null
                ? context.timestamp
                : valueAndTimestamp.timestamp;

            return ValueAndTimestamp<KeyValue<K1, V1>>.make(
                mapped,
                timeStamp);
        }

        public void close()
        {
            parentGetter.close();
        }
    }
}
