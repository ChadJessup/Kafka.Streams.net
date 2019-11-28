﻿using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Window;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamKStreamJoinProcessor<K, V1, V2, R> : AbstractProcessor<K, V1>
    {
        private IWindowStore<K, V2> otherWindow;
        private readonly bool outer;
        private readonly IValueJoiner<V1, V2, R> joiner;
        private readonly TimeSpan joinBeforeMs;
        private readonly TimeSpan joinAfterMs;

        public KStreamKStreamJoinProcessor(
            bool outer,
            IValueJoiner<V1, V2, R> joiner,
            TimeSpan joinBeforeMs,
            TimeSpan joinAfterMs)
        {
            this.outer = outer;
            this.joiner = joiner;
            this.joinBeforeMs = joinBeforeMs;
            this.joinAfterMs = joinAfterMs;
        }

        public override void init(IProcessorContext context)
        {
            base.init(context);
            otherWindow = (IWindowStore<K, V2>)context.getStateStore(otherWindow.name);
        }

        public override void process(K key, V1 value)
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

            bool needOuterJoin = outer;

            long inputRecordTimestamp = context.timestamp;
            long timeFrom = Math.Max(0L, inputRecordTimestamp - (long)joinBeforeMs.TotalMilliseconds);
            long timeTo = Math.Max(0L, inputRecordTimestamp + (long)joinAfterMs.TotalMilliseconds);

            using IWindowStoreIterator<V2> iter = otherWindow.fetch(key, timeFrom, timeTo);
            {
                while (iter.MoveNext())
                {
                    needOuterJoin = false;
                    KeyValue<long, V2> otherRecord = iter.Current;
                    context.forward(
                        key,
                        joiner.apply(value, otherRecord.Value),
                        To.All().WithTimestamp(Math.Max(inputRecordTimestamp, otherRecord.Key)));
                }

                if (needOuterJoin)
                {
                    context.forward(key, joiner.apply(value, default));
                }
            }
        }
    }
}
