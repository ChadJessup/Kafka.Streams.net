using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.KStream.Internals.Suppress
{
    public class KTableSuppressProcessor<K, V> : IKeyValueProcessor<K, Change<V>>
    {
        private readonly long maxRecords;
        private readonly long maxBytes;
        private readonly long suppressDurationMillis;
        private readonly ITimeDefinition<K, V> bufferTimeDefinition;
        private readonly BufferFullStrategy bufferFullStrategy;
        private readonly bool safeToDropTombstones;
        private readonly string storeName;

        //private TimeOrderedKeyValueBuffer<K, V> buffer;
        private IInternalProcessorContext internalProcessorContext;
        private long observedStreamTime = -1L;// ConsumeResult.NO_TIMESTAMP;

        //public KTableSuppressProcessor(SuppressedInternal<K, V> suppress, string storeName)
        //{
        //    this.storeName = storeName;
        //    //requireNonNull(suppress);
        //    maxRecords = suppress.bufferConfig.maxRecords();
        //    maxBytes = suppress.bufferConfig.maxBytes();
        //    suppressDurationMillis = suppress.timeToWaitForMoreEvents().toMillis();
        //    bufferTimeDefinition = suppress.timeDefinition;
        //    bufferFullStrategy = suppress.bufferConfig.bufferFullStrategy();
        //    safeToDropTombstones = suppress.safeToDropTombstones;
        //}

        public void init(IProcessorContext context)
        {
            internalProcessorContext = (IInternalProcessorContext)context;
//            suppressionEmitSensor = Sensors.suppressionEmitSensor(internalProcessorContext);

            //buffer = requireNonNull((TimeOrderedKeyValueBuffer<K, V>)context.getStateStore(storeName));
            //buffer.setSerdesIfNull((ISerde<K>)context.keySerde, (ISerde<V>)context.valueSerde);
        }


        public void process(K key, Change<V> value)
        {
            observedStreamTime = Math.Max(observedStreamTime, internalProcessorContext.timestamp);
            buffer(key, value);
            enforceConstraints();
        }

        private void buffer(K key, Change<V> value)
        {
            long bufferTime = bufferTimeDefinition.time(internalProcessorContext, key);

          //  buffer.Add(bufferTime, key, value, internalProcessorContext.recordContext());
        }

        private void enforceConstraints()
        {
            long streamTime = observedStreamTime;
            long expiryTime = streamTime - suppressDurationMillis;

            //buffer.evictWhile(() => buffer.minTimestamp() <= expiryTime, this.emit);

            if (overCapacity())
            {
                switch (bufferFullStrategy)
                {
                    //case EMIT:
                    //    buffer.evictWhile(this.overCapacity, this.emit);
                    //    return;
                    //case SHUT_DOWN:
                    //    throw new StreamsException(string.Format(
                    //        "%s buffer exceeded its max capacity. Currently [%d/%d] records and [%d/%d] bytes.",
                    //        internalProcessorContext.currentNode().name,
                    //        //buffer.numRecords(), maxRecords,
                    //        //buffer.bufferSize(), maxBytes));
                    //        null, null));

                    default:
                        throw new InvalidOperationException(
                            "The bufferFullStrategy [" + bufferFullStrategy +
                                "] is not implemented. This is a bug in Kafka Streams.");
                }
            }
        }

        private bool overCapacity()
        {
            return false; // buffer.numRecords() > maxRecords || buffer.bufferSize() > maxBytes;
        }

        private void emit(Eviction<K, V> toEmit)
        {
            if (ShouldForward(toEmit.value))
            {
                ProcessorRecordContext prevRecordContext = internalProcessorContext.recordContext;
                internalProcessorContext.setRecordContext(toEmit.recordContext);

                try
                {
                    internalProcessorContext.forward(toEmit.key, toEmit.value);
                }
                finally
                {
                    internalProcessorContext.setRecordContext(prevRecordContext);
                }
            }
        }

        private bool ShouldForward(Change<V> value)
        {
            return value.newValue != null || !safeToDropTombstones;
        }

        public void close()
        {
        }
    }
}
