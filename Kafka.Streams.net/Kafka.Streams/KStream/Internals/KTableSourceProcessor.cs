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

        public override void Process(K key, V value)
        {
            // if the key is null, then ignore the record
            if (key == null)
            {
                this.logger.LogWarning(
                    "Skipping record due to null key. topic=[{}] partition=[{}] offset=[{}]",
                    this.Context.Topic, this.Context.Partition, this.Context.Offset);

                return;
            }

            if (this.queryableName != null)
            {
                ValueAndTimestamp<V> oldValueAndTimestamp = this.store.Get(key);

                V oldValue;
                if (oldValueAndTimestamp != null)
                {
                    oldValue = oldValueAndTimestamp.Value;
                    
                    if (this.Context.Timestamp < oldValueAndTimestamp.Timestamp)
                    {
                        this.logger.LogWarning("Detected out-of-order KTable update for {} at offset {}, partition {}.",
                            this.store.Name, this.Context.Offset, this.Context.Partition);
                    }
                }
                else
                {
                    oldValue = default;
                }

                this.store.Add(key, ValueAndTimestamp.Make(value, this.Context.Timestamp));

                this.tupleForwarder.MaybeForward(key, value, oldValue);
            }
            else
            {

                this.Context.Forward(key, new Change<V>(value, default));
            }
        }
    }
}
