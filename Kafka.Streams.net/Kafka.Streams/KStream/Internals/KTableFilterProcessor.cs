using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableFilterProcessor<K, V> : AbstractProcessor<K, IChange<V>>
    {
        private readonly KafkaStreamsContext context;
        private readonly string? queryableName;
        private readonly bool sendOldValues;
        private readonly bool filterNot;
        private readonly Func<K, V, bool> predicate;
        private ITimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;

        public KTableFilterProcessor(
            KafkaStreamsContext context,
            string? queryableName, 
            bool sendOldValues,
            bool filterNot,
            Func<K, V, bool> predicate)
        {
            this.context = context;
            this.queryableName = queryableName;
            this.sendOldValues = sendOldValues;
            this.FilterNot = filterNot;
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
                this.store = (ITimestampedKeyValueStore<K, V>)context.GetStateStore(this.queryableName);
                this.tupleForwarder = new TimestampedTupleForwarder<K, V>(
                    this.store,
                    context,
                    new TimestampedCacheFlushListener<K, V>(context),
                    this.sendOldValues);
            }
        }

        public override void Process(K key, IChange<V> change)
        {
            if (change is null)
            {
                throw new ArgumentNullException(nameof(change));
            }

            V newValue = this.ComputeValue(key, change.NewValue);
            V oldValue = this.sendOldValues
                ? this.ComputeValue(key, change.OldValue)
                : default;

            if (this.sendOldValues && oldValue == null && newValue == null)
            {
                return; // unnecessary to forward here.
            }

            if (this.queryableName != null)
            {
                this.store.Add(key, ValueAndTimestamp.Make(newValue, this.Context.Timestamp));
                this.tupleForwarder.MaybeForward(key, newValue, oldValue);
            }
            else
            {
                this.Context.Forward(key, new Change<V>(newValue, oldValue));
            }
        }

        private V ComputeValue(K key, V value)
        {
            V newValue = default;

            if (value != null && (this.FilterNot ^ this.predicate(key, value)))
            {
                newValue = value;
            }

            return newValue;
        }

        private IValueAndTimestamp<V>? ComputeValue(
            K key,
            IValueAndTimestamp<V> valueAndTimestamp)
        {
            IValueAndTimestamp<V>? newValueAndTimestamp = null;

            if (valueAndTimestamp != null)
            {
                V value = valueAndTimestamp.Value;
                if (this.FilterNot ^ this.predicate(key, value))
                {
                    newValueAndTimestamp = valueAndTimestamp;
                }
            }

            return newValueAndTimestamp;
        }
    }
}
