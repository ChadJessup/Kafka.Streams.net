using Kafka.Streams.KStream.Interfaces;
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
        private readonly IValueMapperWithKey<K, V, V1> mapper;
        private readonly string queryableName;
        private readonly bool sendOldValues;

        public KTableMapValuesProcessor(IValueMapperWithKey<K, V, V1> mapper)
        {
            this.mapper = mapper;
        }

        public override void Init(IProcessorContext context)
        {
            if (context is null)
            {
                throw new System.ArgumentNullException(nameof(context));
            }

            base.Init(context);
            if (queryableName != null)
            {
                store = (ITimestampedKeyValueStore<K, V1>)context.GetStateStore(queryableName);

                tupleForwarder = new TimestampedTupleForwarder<K, V1>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<K, V1>(context),
                    sendOldValues);
            }
        }

        public override void Process(K key, IChange<V> change)
        {
            if (change is null)
            {
                throw new System.ArgumentNullException(nameof(change));
            }

            V1 newValue = ComputeValue(key, change.NewValue);
            V1 oldValue = sendOldValues
                ? ComputeValue(key, change.OldValue)
                : default;

            if (queryableName != null)
            {
                store.Add(key, ValueAndTimestamp.Make(newValue, Context.Timestamp));
                tupleForwarder.MaybeForward(key, newValue, oldValue);
            }
            else
            {
                Context.Forward(key, new Change<V1>(newValue, oldValue));
            }
        }

        private V1 ComputeValue(K key, V value)
        {
            V1 newValue = default;

            if (value != null)
            {
                newValue = mapper.Apply(key, value);
            }

            return newValue;
        }
    }
}
