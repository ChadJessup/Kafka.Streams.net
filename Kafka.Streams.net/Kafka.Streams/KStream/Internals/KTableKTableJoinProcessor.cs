using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Microsoft.Extensions.Logging;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableJoinProcessor<K, R, V1, V2> : AbstractProcessor<K, Change<V1>>
    {
        private readonly IKTableValueGetter<K, V2> valueGetter;
        private readonly string storeName;
        private readonly bool sendOldValues;
        private readonly IValueJoiner<V1, V2, R> joiner;

        public KTableKTableJoinProcessor(
            IKTableValueGetter<K, V2> valueGetter,
            IValueJoiner<V1, V2, R> joiner,
            string storeName,
            bool sendOldValues)
        {
            this.sendOldValues = sendOldValues;
            this.valueGetter = valueGetter;
            this.storeName = storeName;
            this.joiner = joiner;
        }

        public override void Init(IProcessorContext context)
        {
            base.Init(context);
            valueGetter.Init(context, this.storeName);
        }

        public override void Process(K key, Change<V1> change)
        {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            if (key == null)
            {
                //this.logger.LogWarning(
                //    "Skipping record due to null key. change=[{}] topic=[{}] partition=[{}] offset=[{}]",
                //    change, context.Topic, context.partition, context.offset);

                return;
            }

            if (change is null)
            {
                throw new ArgumentNullException(nameof(change));
            }

            long resultTimestamp;

            var valueAndTimestampRight = valueGetter.Get(key);
            V2 valueRight = ValueAndTimestamp.GetValueOrNull(valueAndTimestampRight);
            if (valueRight == null)
            {
                return;
            }

            R newValue = default;
            R oldValue = default;
            resultTimestamp = Math.Max(Context.Timestamp, valueAndTimestampRight.Timestamp);

            if (change.NewValue != null)
            {
                newValue = joiner.Apply(change.NewValue, valueRight);
            }

            if (sendOldValues && change.OldValue != null)
            {
                oldValue = joiner.Apply(change.OldValue, valueRight);
            }

            Context.Forward(key, new Change<R>(newValue, oldValue), To.All().WithTimestamp(resultTimestamp));
        }

        public override void Close()
        {
            valueGetter.Close();
        }
    }
}
