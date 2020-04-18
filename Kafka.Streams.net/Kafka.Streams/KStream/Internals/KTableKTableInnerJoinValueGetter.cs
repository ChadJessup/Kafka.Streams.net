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
        private readonly KeyValueMapper<K, V1, K> keyValueMapper;
        private readonly ValueJoiner<V1, V2, R> joiner;

        public KTableKTableInnerJoinValueGetter(
            IKTableValueGetter<K, V1> valueGetter1,
            IKTableValueGetter<K, V2> valueGetter2,
            KeyValueMapper<K, V1, K> keyValueMapper,
            ValueJoiner<V1, V2, R> joiner)
        {
            this.valueGetter1 = valueGetter1;
            this.valueGetter2 = valueGetter2;
            this.keyValueMapper = keyValueMapper;
            this.joiner = joiner;
        }

        public void Init(IProcessorContext context, string storeName)
        {
            this.valueGetter1.Init(context, storeName);
            this.valueGetter2.Init(context, storeName);
        }

        public IValueAndTimestamp<R>? Get(K key)
        {
            var valueAndTimestamp1 = this.valueGetter1.Get(key);
            V1 value1 = ValueAndTimestamp.GetValueOrNull(valueAndTimestamp1);

            if (value1 != null)
            {
                var valueAndTimestamp2 = this.valueGetter2.Get(this.keyValueMapper.Apply(key, value1));
                V2 value2 = ValueAndTimestamp.GetValueOrNull(valueAndTimestamp2);

                if (value2 != null)
                {
                    return ValueAndTimestamp.Make(
                        this.joiner(value1, value2),
                        valueAndTimestamp1?.Timestamp.GetNewest(valueAndTimestamp2.Timestamp) ?? valueAndTimestamp2.Timestamp);
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

        public void Close()
        {
            this.valueGetter1.Close();
            this.valueGetter2.Close();
        }
    }
}
