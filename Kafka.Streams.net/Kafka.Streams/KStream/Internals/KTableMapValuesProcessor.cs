using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMapValuesProcessor<K, V, V1> : AbstractProcessor<K, IChange<V>>
    {
        private ITimestampedKeyValueStore<K, V1> store;
        private TimestampedTupleForwarder<K, V1> tupleForwarder;
        private readonly KafkaStreamsContext context;
        private readonly ValueMapperWithKey<K, V, V1> mapper;
        private readonly string queryableName;
        private readonly bool sendOldValues;

        public KTableMapValuesProcessor(
            KafkaStreamsContext context,
            ValueMapperWithKey<K, V, V1> mapper)
        {
            this.context = context;
            this.mapper = mapper;
        }

        public override void Init(IProcessorContext context)
        {
            if (context is null)
            {
                throw new System.ArgumentNullException(nameof(context));
            }

            base.Init(context);
            if (this.queryableName != null)
            {
                this.store = (ITimestampedKeyValueStore<K, V1>)context.GetStateStore(this.queryableName);

                this.tupleForwarder = new TimestampedTupleForwarder<K, V1>(
                    this.store,
                    context,
                    new TimestampedCacheFlushListener<K, V1>(context),
                    this.sendOldValues);
            }
        }

        public override void Process(K key, IChange<V> change)
        {
            if (change is null)
            {
                throw new System.ArgumentNullException(nameof(change));
            }

            V1 newValue = this.ComputeValue(key, change.NewValue);
            V1 oldValue = this.sendOldValues
                ? this.ComputeValue(key, change.OldValue)
                : default;

            if (this.queryableName != null)
            {
                this.store.Add(key, ValueAndTimestamp.Make(newValue, this.Context.Timestamp));
                this.tupleForwarder.MaybeForward(key, newValue, oldValue);
            }
            else
            {
                this.Context.Forward(key, new Change<V1>(newValue, oldValue));
            }
        }

        private V1 ComputeValue(K key, V value)
        {
            V1 newValue = default;

            if (value != null)
            {
                newValue = this.mapper(key, value);
            }

            return newValue;
        }
    }
}
