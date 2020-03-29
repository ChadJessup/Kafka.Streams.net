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
        private ITimestampedKeyValueStore<K, V> store;
        private TimestampedTupleForwarder<K, V> tupleForwarder;
        private readonly string queryableName;
        private readonly bool sendOldValues;

        public KTableSourceProcessor(
            ILogger<KTableSourceProcessor<K, V>> logger,
            string queryableName,
            bool sendOldValues)
        {
            this.logger = logger;
            this.queryableName = queryableName;
            this.sendOldValues = sendOldValues;
        }

        public override void Init(IProcessorContext context)
        {
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

        public override void Process(K key, V value)
        {
            // if the key is null, then ignore the record
            if (key == null)
            {
                logger.LogWarning(
                    "Skipping record due to null key. topic=[{}] partition=[{}] offset=[{}]",
                    context.Topic, context.partition, context.offset);

                return;
            }

            if (queryableName != null)
            {
                ValueAndTimestamp<V> oldValueAndTimestamp = store.Get(key);

                V oldValue;
                if (oldValueAndTimestamp != null)
                {
                    oldValue = oldValueAndTimestamp.value;
                    
                    if (context.timestamp < oldValueAndTimestamp.timestamp)
                    {
                        logger.LogWarning("Detected out-of-order KTable update for {} at offset {}, partition {}.",
                            store.name, context.offset, context.partition);
                    }
                }
                else
                {
                    oldValue = default;
                }

                store.Add(key, ValueAndTimestamp<V>.make(value, context.timestamp));

                tupleForwarder.maybeForward(key, value, oldValue);
            }
            else
            {

                context.forward(key, new Change<V>(value, default));
            }
        }
    }
}
