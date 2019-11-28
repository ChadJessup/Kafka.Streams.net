using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMapValuesProcessor<K, V, V1> : AbstractProcessor<K, Change<V>>
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

        public override void init(IProcessorContext context)
        {
            base.init(context);
            if (queryableName != null)
            {
                store = (ITimestampedKeyValueStore<K, V1>)context.getStateStore(queryableName);

                tupleForwarder = new TimestampedTupleForwarder<K, V1>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<K, V1>(context),
                    sendOldValues);
            }
        }


        public override void process(K key, Change<V> change)
        {
            V1 newValue = computeValue(key, change.newValue);
            V1 oldValue = sendOldValues
                ? computeValue(key, change.oldValue)
                : default;

            if (queryableName != null)
            {
                store.Add(key, ValueAndTimestamp<V1>.make(newValue, context.timestamp));
                tupleForwarder.maybeForward(key, newValue, oldValue);
            }
            else
            {
                context.forward(key, new Change<V1>(newValue, oldValue));
            }
        }

        private V1 computeValue(K key, V value)
        {
            V1 newValue = default;

            if (value != null)
            {
                newValue = mapper.apply(key, value);
            }

            return newValue;
        }
    }
}