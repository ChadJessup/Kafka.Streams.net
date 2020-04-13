using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Windowed;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamKStreamJoinProcessor<K, V1, V2, R> : AbstractProcessor<K, V1>
    {
        private IWindowStore<K, V2> otherWindow;
        private readonly KafkaStreamsContext context;
        private readonly IValueJoiner<V1, V2, R> joiner;
        private readonly TimeSpan joinBefore;
        private readonly TimeSpan joinAfter;
        private readonly bool outer;

        public KStreamKStreamJoinProcessor(
            KafkaStreamsContext context,
            bool outer,
            IValueJoiner<V1, V2, R> joiner,
            TimeSpan joinBeforeMs,
            TimeSpan joinAfterMs)
        {
            this.context = context;
            this.outer = outer;
            this.joiner = joiner;
            this.joinBefore = joinBeforeMs;
            this.joinAfter = joinAfterMs;
        }

        public override void Init(IProcessorContext context)
        {
            base.Init(context);
            this.otherWindow = (IWindowStore<K, V2>)context.GetStateStore(this.otherWindow.Name);
        }

        public override void Process(K key, V1 value)
        {
            // we do join iff keys are equal, thus, if key is null we cannot join and just ignore the record
            //
            // we also ignore the record if value is null, because in a key-value data model a null-value indicates
            // an empty message (ie, there is nothing to be joined) -- this contrast SQL NULL semantics
            // furthermore, on left/outer joins 'null' in ValueJoiner#apply() indicates a missing record --
            // thus, to be consistent and to avoid ambiguous null semantics, null values are ignored
            if (key == null || value == null)
            {
                //LOG.LogWarning(
                //    "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                //    key, value, context.Topic, context.partition, context.offset);

                return;
            }

            var needOuterJoin = this.outer;

            var inputRecordTimestamp = this.Context.Timestamp;
            var timeFrom = (inputRecordTimestamp - this.joinBefore).GetNewest(DateTime.MinValue);
            var timeTo = (inputRecordTimestamp + this.joinAfter).GetNewest(DateTime.MinValue);

            using IWindowStoreIterator<V2> iter = this.otherWindow.Fetch(key, timeFrom, timeTo);
            {
                while (iter.MoveNext())
                {
                    needOuterJoin = false;
                    KeyValuePair<DateTime, V2> otherRecord = iter.Current;
                    this.Context.Forward(
                        key,
                        this.joiner.Apply(value, otherRecord.Value),
                        To.All().WithTimestamp(inputRecordTimestamp.GetNewest(otherRecord.Key)));
                }

                if (needOuterJoin)
                {
                    this.Context.Forward(key, this.joiner.Apply(value, default));
                }
            }
        }
    }
}
