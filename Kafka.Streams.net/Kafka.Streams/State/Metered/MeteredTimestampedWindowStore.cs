using System;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.State.Metered
{
    public class MeteredTimestampedWindowStore<K, V>
        : MeteredWindowStore<K, IValueAndTimestamp<V>>,
        ITimestampedWindowStore<K, V>
    {
        public MeteredTimestampedWindowStore(
            KafkaStreamsContext context,
            IWindowStore<Bytes, byte[]> inner,
            TimeSpan windowSize,
            ISerde<K> keySerde,
            ISerde<IValueAndTimestamp<V>> valueSerde)
            : base(
                  context,
                  inner,
                  windowSize,
                  keySerde,
                  valueSerde)
        {
        }

        protected override void InitStoreSerde(IProcessorContext context)
        {
            this.serdes = new StateSerdes<K, IValueAndTimestamp<V>>(
                ProcessorStateManager.StoreChangelogTopic(context.ApplicationId, this.Name),
                this.KeySerde ?? (ISerde<K>)context.KeySerde,
                this.ValueSerde ?? new ValueAndTimestampSerde<V>((ISerde<V>)context.ValueSerde));
        }
    }
}
