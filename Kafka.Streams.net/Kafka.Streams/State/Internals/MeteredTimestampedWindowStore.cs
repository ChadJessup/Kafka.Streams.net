
//    public class MeteredTimestampedWindowStore<K, V>
//        : MeteredWindowStore<K, ValueAndTimestamp<V>>
//    , ITimestampedWindowStore<K, V>
//    {

//        MeteredTimestampedWindowStore(IWindowStore<Bytes, byte[]> inner,
//                                      long windowSizeMs,
//                                      string metricScope,
//                                      ITime time,
//                                      ISerde<K> keySerde,
//                                      ISerde<ValueAndTimestamp<V>> valueSerde)
//            : base(inner, windowSizeMs, metricScope, time, keySerde, valueSerde)
//        {
//        }



//        void initStoreSerde(IProcessorContext<K, V> context)
//        {
//            serdes = new StateSerdes<>(
//                ProcessorStateManager.storeChangelogTopic(context.applicationId(), name),
//                keySerde == null ? (ISerde<K>)context.keySerde : keySerde,
//                valueSerde == null ? new ValueAndTimestampSerde<>((ISerde<V>)context.valueSerde) : valueSerde);
//        }
//    }
//}