using System;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableOuterJoinValueGetter<K, R, V1, V2> : IKTableValueGetter<K, R>
    {
        private readonly IKTableValueGetter<K, V1> valueGetter1;
        private readonly IKTableValueGetter<K, V2> valueGetter2;
        private readonly ValueJoiner<V1, V2, R> joiner;

        public KTableKTableOuterJoinValueGetter(
            IKTableValueGetter<K, V1> valueGetter1,
            IKTableValueGetter<K, V2> valueGetter2,
            ValueJoiner<V1, V2, R> joiner)
        {
            this.valueGetter1 = valueGetter1 ?? throw new ArgumentNullException(nameof(valueGetter1));
            this.valueGetter2 = valueGetter2 ?? throw new ArgumentNullException(nameof(valueGetter2));
            this.joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
        }

        public void Init(IProcessorContext context, string? storeName)
        {
            this.valueGetter1.Init(context, storeName);
            this.valueGetter2.Init(context, storeName);
        }

        public IValueAndTimestamp<R> Get(K key)
        {
            R newValue = default;

            IValueAndTimestamp<V1>? valueAndTimestamp1 = this.valueGetter1.Get(key);
            V1 value1;
            DateTime timestamp1;
            if (valueAndTimestamp1 == null)
            {
                value1 = default;
                timestamp1 = DateTime.MinValue;
            }
            else
            {

                value1 = valueAndTimestamp1.Value;
                timestamp1 = valueAndTimestamp1.Timestamp;
            }

            IValueAndTimestamp<V2>? valueAndTimestamp2 = this.valueGetter2.Get(key);
            V2 value2;
            DateTime timestamp2;
            if (valueAndTimestamp2 == null)
            {
                value2 = default;
                timestamp2 = DateTime.MinValue;
            }
            else
            {
                value2 = valueAndTimestamp2.Value;
                timestamp2 = valueAndTimestamp2.Timestamp;
            }

            if (value1 != null || value2 != null)
            {
                newValue = this.joiner(value1!, value2!);
            }

            return ValueAndTimestamp.Make(newValue!, timestamp1.GetNewest(timestamp2));
        }

        public void Close()
        {
            this.valueGetter1.Close();
            this.valueGetter2.Close();
        }

        public void Init(IProcessorContext processorContext)
        {
            throw new NotImplementedException();
        }
    }
}
