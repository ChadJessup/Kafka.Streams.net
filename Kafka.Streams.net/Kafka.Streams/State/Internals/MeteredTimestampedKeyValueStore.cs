
//    public class MeteredTimestampedKeyValueStore<K, V>
//        : MeteredKeyValueStore<K, ValueAndTimestamp<V>>
//    : ITimestampedKeyValueStore<K, V>
//    {

//        MeteredTimestampedKeyValueStore(IKeyValueStore<Bytes, byte[]> inner,
//                                        string metricScope,
//                                        ITime time,
//                                        ISerde<K> keySerde,
//                                        ISerde<ValueAndTimestamp<V>> valueSerde)
//        {
//            base(inner, metricScope, time, keySerde, valueSerde);
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