using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableJoinMergeProcessor<K, V> : AbstractProcessor<K, Change<V>>
    {
        private readonly string queryableName;
        private ITimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;
        private readonly bool sendOldValues;

        public KTableKTableJoinMergeProcessor(string queryableName)
        {
            this.queryableName = queryableName;
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
                store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(queryableName);
                tupleForwarder = new TimestampedTupleForwarder<K, V>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<K, V>(context),
                    sendOldValues);
            }
        }

        public override void Process(K key, Change<V> value)
        {
            if (queryableName != null)
            {
                store.Add(key, ValueAndTimestamp<V>.make(value.newValue, context.timestamp));

                tupleForwarder.maybeForward(key, value.newValue, sendOldValues
                    ? value.oldValue
                    : default);
            }
            else
            {
                if (sendOldValues)
                {
                    context.forward(key, value);
                }
                else
                {
                    context.forward(key, new Change<V>(value.newValue, default));
                }
            }
        }
    }
}
