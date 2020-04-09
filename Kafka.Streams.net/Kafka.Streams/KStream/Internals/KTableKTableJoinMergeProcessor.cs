using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableKTableJoinMergeProcessor<K, V> : AbstractProcessor<K, IChange<V>>
    {
        private readonly KafkaStreamsContext context;
        private readonly string queryableName;
        private ITimestampedKeyValueStore<K, V>? store;
        private TimestampedTupleForwarder<K, V>? tupleForwarder;
        private readonly bool sendOldValues;

        public KTableKTableJoinMergeProcessor(KafkaStreamsContext context, string queryableName)
        {
            this.context = context;
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
                store = (ITimestampedKeyValueStore<K, V>)context.GetStateStore(this.context, queryableName);
                tupleForwarder = new TimestampedTupleForwarder<K, V>(
                    store,
                    context,
                    new TimestampedCacheFlushListener<K, V>(context),
                    sendOldValues);
            }
        }

        public override void Process(K key, IChange<V> value)
        {
            if (value is null)
            {
                throw new System.ArgumentNullException(nameof(value));
            }

            if (queryableName != null)
            {
                store.Add(key, ValueAndTimestamp.Make(value.NewValue, Context.Timestamp));

                tupleForwarder.MaybeForward(key, value.NewValue, sendOldValues
                    ? value.OldValue
                    : default);
            }
            else
            {
                if (sendOldValues)
                {
                    Context.Forward(key, value);
                }
                else
                {
                    Context.Forward(key, new Change<V>(value.NewValue));
                }
            }
        }
    }
}
