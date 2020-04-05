using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableFilterProcessor<K, V> : AbstractProcessor<K, Change<V>>
    {
        private readonly string? queryableName;
        private readonly bool sendOldValues;
        private readonly bool filterNot;
        private readonly Func<K, V, bool> predicate;
        private ITimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        public KTableFilterProcessor(
            string? queryableName, 
            bool sendOldValues,
            bool filterNot,
            Func<K, V, bool> predicate)
        {
            this.queryableName = queryableName;
            this.sendOldValues = sendOldValues;
            this.filterNot = filterNot;
            this.predicate = predicate;
        }

        public override void Init(IProcessorContext context)
        {
            if (context is null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            base.Init(context);
            if (this.queryableName != null)
            {
                store = (ITimestampedKeyValueStore<K, V>)context.GetStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<K, V>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<K, V>(context),
                    sendOldValues);
            }
        }

        public override void Process(K key, Change<V> change)
        {
            if (change is null)
            {
                throw new ArgumentNullException(nameof(change));
            }

            V newValue = ComputeValue(key, change.newValue);
            V oldValue = sendOldValues
                ? ComputeValue(key, change.oldValue)
                : default;

            if (sendOldValues && oldValue == null && newValue == null)
            {
                return; // unnecessary to forward here.
            }

            if (queryableName != null)
            {
                store.Add(key, ValueAndTimestamp.Make(newValue, context.timestamp));
                tupleForwarder.MaybeForward(key, newValue, oldValue);
            }
            else
            {
                context.Forward(key, new Change<V>(newValue, oldValue));
            }
        }

        private V ComputeValue(K key, V value)
        {
            V newValue = default;

            if (value != null && (filterNot ^ predicate(key, value)))
            {
                newValue = value;
            }

            return newValue;
        }

        private ValueAndTimestamp<V>? ComputeValue(
            K key,
            ValueAndTimestamp<V> valueAndTimestamp)
        {
            ValueAndTimestamp<V>? newValueAndTimestamp = null;

            if (valueAndTimestamp != null)
            {
                V value = valueAndTimestamp.value;
                if (filterNot ^ predicate(key, value))
                {
                    newValueAndTimestamp = valueAndTimestamp;
                }
            }

            return newValueAndTimestamp;
        }
    }
}
