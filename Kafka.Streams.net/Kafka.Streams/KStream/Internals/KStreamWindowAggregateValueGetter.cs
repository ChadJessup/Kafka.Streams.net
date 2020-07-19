using System;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.KStream.Internals
{
    public class KStreamWindowAggregateValueGetter<K, V, Agg> : IKTableValueGetter<IWindowed<K>, Agg>
    {
        private ITimestampedWindowStore<K, Agg> windowStore;
        private readonly KafkaStreamsContext context;

        public KStreamWindowAggregateValueGetter(KafkaStreamsContext context)
        {
            this.context = context ?? throw new ArgumentNullException(nameof(context));
        }

        public void Init(IProcessorContext processorContext, string? storeName)
        {
            if (processorContext is null)
            {
                throw new ArgumentNullException(nameof(processorContext));
            }

            this.windowStore = (ITimestampedWindowStore<K, Agg>)processorContext.GetStateStore(storeName);
        }

        public IValueAndTimestamp<Agg> Get(IWindowed<K> windowedKey)
        {
            if (windowedKey is null)
            {
                throw new System.ArgumentNullException(nameof(windowedKey));
            }

            K key = windowedKey.Key;
            var window = windowedKey.Window;

            return this.windowStore.Fetch(key, window.StartTime);
        }

        public void Close() { }

        public void Init(IProcessorContext processorContext)
        {
            throw new NotImplementedException();
        }
    }
}
