using Kafka.Streams.Errors;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using System;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public class KTableSuppressProcessor<K, V> : IKeyValueProcessor<K, Change<V>>
    {
        private readonly long suppressDurationMillis;
        private readonly ITimeDefinition<K> bufferTimeDefinition;
        private readonly BufferFullStrategy bufferFullStrategy;
        private readonly bool safeToDropTombstones;
        private readonly string storeName;

        // private ITimeOrderedKeyValueBuffer<K, V> buffer;
        private IInternalProcessorContext internalProcessorContext;
        private long observedStreamTime = -1L;// ConsumeResult.NO_TIMESTAMP;

        public KTableSuppressProcessor(
            SuppressedInternal<K> suppress,
            string storeName)
        {
            if (suppress is null)
            {
                throw new ArgumentNullException(nameof(suppress));
            }

            this.storeName = storeName;
            // this.MaxRecords = suppress.bufferConfig.MaxRecords;
            // this.MaxBytes = suppress.bufferConfig.MaxBytes;
            suppressDurationMillis = (long)suppress.TimeToWaitForMoreEvents().TotalMilliseconds;
            bufferTimeDefinition = suppress.timeDefinition;
            bufferFullStrategy = suppress.bufferConfig.BufferFullStrategy;
            safeToDropTombstones = suppress.safeToDropTombstones;
        }

        public void Init(IProcessorContext context)
        {
            internalProcessorContext = (IInternalProcessorContext)context;

            //buffer = requireNonNull((ITimeOrderedKeyValueBuffer<K, V>)context.getStateStore(storeName));
            //buffer.setSerdesIfNull((ISerde<K>)context.keySerde, (ISerde<V>)context.valueSerde);
        }


        public void Process(K key, Change<V> value)
        {
            observedStreamTime = Math.Max(observedStreamTime, internalProcessorContext.timestamp);
            Buffer(key, value);
            EnforceConstraints();
        }

        private void Buffer(K key, Change<V> value)
        {
            var bufferTime = bufferTimeDefinition.Time(internalProcessorContext, key);

            // buffer.Add(bufferTime, key, value, internalProcessorContext.recordContext());
        }

        private void EnforceConstraints()
        {
            var streamTime = observedStreamTime;
            var expiryTime = streamTime - suppressDurationMillis;

            //buffer.evictWhile(() => buffer.minTimestamp() <= expiryTime, this.emit);

            if (OverCapacity())
            {
                switch (bufferFullStrategy)
                {
                    case BufferFullStrategy.EMIT:
                        //    buffer.evictWhile(this.overCapacity, this.emit);
                        return;
                    case BufferFullStrategy.SPILL_TO_DISK:
                        break;
                    case BufferFullStrategy.SHUT_DOWN:
                        throw new StreamsException(string.Format(
                            "%s buffer exceeded its max capacity. Currently [%d/%d] records and [%d/%d] bytes.",
                            // internalProcessorContext.currentNode().name,
                            //buffer.numRecords(), maxRecords,
                            //buffer.bufferSize(), maxBytes));
                            null, null));

                    default:
                        throw new InvalidOperationException(
                            "The bufferFullStrategy [" + bufferFullStrategy +
                                "] is not implemented. This is a bug in Kafka Streams.");
                }
            }
        }

        private bool OverCapacity()
        {
            return false; // buffer.numRecords() > maxRecords || buffer.bufferSize() > maxBytes;
        }

        private bool ShouldForward(Change<V> value)
        {
            return value.newValue != null || !safeToDropTombstones;
        }

        public void Close()
        {
        }

        public void Process<K1, V1>(K1 key, V1 value)
        {
            this.Process(key, value);
        }
    }
}
