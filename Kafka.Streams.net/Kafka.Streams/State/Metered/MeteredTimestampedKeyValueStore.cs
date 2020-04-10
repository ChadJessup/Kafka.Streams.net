using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.State.Metered
{
    public class MeteredTimestampedKeyValueStore<K, V>
        : MeteredKeyValueStore<K, IValueAndTimestamp<V>>,
        ITimestampedKeyValueStore<K, V>
    {
        public MeteredTimestampedKeyValueStore(
            KafkaStreamsContext context,
            IKeyValueStore<Bytes, byte[]> inner,
            ISerde<K> keySerde,
            ISerde<IValueAndTimestamp<V>> valueSerde)
            : base(context, inner, keySerde, valueSerde)
        {
        }

        protected override void InitStoreSerde(IProcessorContext context)
        {
            var ks = this.KeySerde ?? (ISerde<K>)context.KeySerde;
            var vs = this.ValueSerde ?? new ValueAndTimestampSerde<V>((ISerde<V>)context.ValueSerde);

            this.Serdes = new StateSerdes<K, IValueAndTimestamp<V>>(
                ProcessorStateManager.StoreChangelogTopic(context.ApplicationId, this.Name),
                ks,
                vs);
        }
    }
}
