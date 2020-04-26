using System;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableOuterJoinProcessor<K, R, V1, V2> : AbstractProcessor<K, IChange<V1>>
    {
        private readonly IKTableValueGetter<K, V2> valueGetter;
        private readonly bool sendOldValues;
        private readonly ValueJoiner<V1, V2, R> joiner;

        public KTableKTableOuterJoinProcessor(
            IKTableValueGetter<K, V2> valueGetter,
            bool sendOldValues,
            ValueJoiner<V1, V2, R> joiner)
        {
            this.valueGetter = valueGetter;
            this.sendOldValues = sendOldValues;
            this.joiner = joiner ?? throw new ArgumentNullException(nameof(joiner));
        }

        public override void Init(IProcessorContext context)
        {
            base.Init(context);
            this.valueGetter.Init(context, null);
        }


        public override void Process(K key, IChange<V1> change)
        {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            if (key == null)
            {
                //LOG.LogWarning(
                //    "Skipping record due to null key. change=[{}] topic=[{}] partition=[{}] offset=[{}]",
                //    change, context.Topic, context.Partition, context.offset()
                //);

                return;
            }

            if (change is null)
            {
                throw new ArgumentNullException(nameof(change));
            }

            R NewValue = default;
            DateTime resultTimestamp;
            R OldValue = default;

            IValueAndTimestamp<V2>? valueAndTimestamp2 = this.valueGetter.Get(key);
            V2 value2 = ValueAndTimestamp.GetValueOrNull(valueAndTimestamp2);

            if (value2 == null)
            {
                if (change.NewValue == null && change.OldValue == null)
                {
                    return;
                }

                resultTimestamp = this.Context.Timestamp;
            }
            else
            {

                resultTimestamp = this.Context.Timestamp.GetNewest(valueAndTimestamp2.Timestamp);
            }

            if (value2 != null || change.NewValue != null)
            {
                NewValue = this.joiner(change.NewValue, value2);
            }

            if (this.sendOldValues && (value2 != null || change.OldValue != null))
            {
                OldValue = this.joiner(change.OldValue, value2);
            }

            this.Context.Forward(key, new Change<R>(NewValue, OldValue), To.All().WithTimestamp(resultTimestamp));
        }

        public override void Close()
        {
            this.valueGetter.Close();
        }
    }
}
