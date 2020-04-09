using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableSourceProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly ILogger<KTableSourceProcessor<K, V>> logger;
        private readonly KafkaStreamsContext context;
        private ITimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;
        private readonly string queryableName;
        private readonly bool sendOldValues;

        public KTableSourceProcessor(
            KafkaStreamsContext context,
            ILogger<KTableSourceProcessor<K, V>> logger,
            string queryableName,
            bool sendOldValues)
        {
            this.logger = logger;
            this.context = context;
            this.queryableName = queryableName;
            this.sendOldValues = sendOldValues;
        }

        public override void Init(IProcessorContext context)
        {
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

        public override void Process(K key, V value)
        {
            // if the key is null, then ignore the record
            if (key == null)
            {
                logger.LogWarning(
                    "Skipping record due to null key. topic=[{}] partition=[{}] offset=[{}]",
                    Context.Topic, Context.Partition, Context.Offset);

                return;
            }

            if (queryableName != null)
            {
                ValueAndTimestamp<V> oldValueAndTimestamp = store.Get(key);

                V oldValue;
                if (oldValueAndTimestamp != null)
                {
                    oldValue = oldValueAndTimestamp.Value;
                    
                    if (Context.Timestamp < oldValueAndTimestamp.Timestamp)
                    {
                        logger.LogWarning("Detected out-of-order KTable update for {} at offset {}, partition {}.",
                            store.Name, Context.Offset, Context.Partition);
                    }
                }
                else
                {
                    oldValue = default;
                }

                store.Add(key, ValueAndTimestamp.Make(value, Context.Timestamp));

                tupleForwarder.MaybeForward(key, value, oldValue);
            }
            else
            {

                Context.Forward(key, new Change<V>(value, default));
            }
        }
    }
}
