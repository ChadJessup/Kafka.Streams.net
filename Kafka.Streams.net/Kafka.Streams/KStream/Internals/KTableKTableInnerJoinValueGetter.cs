using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableInnerJoinValueGetter<K, R, V1, V2> : IKTableValueGetter<K, R>
    {
        private readonly IKTableValueGetter<K, V1> valueGetter1;
        private readonly IKTableValueGetter<K, V2> valueGetter2;
        private readonly IKeyValueMapper<K, V1, K> keyValueMapper;
        private readonly IValueJoiner<V1, V2, R> joiner;

        public KTableKTableInnerJoinValueGetter(
            IKTableValueGetter<K, V1> valueGetter1,
            IKTableValueGetter<K, V2> valueGetter2,
            IKeyValueMapper<K, V1, K> keyValueMapper,
            IValueJoiner<V1, V2, R> joiner)
        {
            this.valueGetter1 = valueGetter1;
            this.valueGetter2 = valueGetter2;
            this.keyValueMapper = keyValueMapper;
            this.joiner = joiner;
        }

        public void init(IProcessorContext context, string storeName)
        {
            valueGetter1.init(context, storeName);
            valueGetter2.init(context, storeName);
        }

        public ValueAndTimestamp<R>? get(K key)
        {
            var valueAndTimestamp1 = valueGetter1.get(key);
            V1 value1 = ValueAndTimestamp.GetValueOrNull(valueAndTimestamp1);

            if (value1 != null)
            {
                var valueAndTimestamp2 = valueGetter2.get(keyValueMapper.Apply(key, value1));
                V2 value2 = ValueAndTimestamp.GetValueOrNull(valueAndTimestamp2);

                if (value2 != null)
                {
                    return ValueAndTimestamp<R>.make(
                        joiner.apply(value1, value2),
                        Math.Max(valueAndTimestamp1?.timestamp ?? 0, valueAndTimestamp2.timestamp));
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }

        public void close()
        {
            valueGetter1.close();
            valueGetter2.close();
        }
    }
}
