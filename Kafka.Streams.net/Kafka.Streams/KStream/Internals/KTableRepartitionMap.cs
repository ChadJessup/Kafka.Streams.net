//using Kafka.Streams.Errors;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.KStream.Mappers;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State;
//using System;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals
//{
//    /**
//    * KTable repartition map functions are not exposed to public APIs, but only used for keyed aggregations.
//    * <p>
//    * Given the input, it can output at most two records (one mapped from old value and one mapped from new value).
//*/
//    public class KTableRepartitionMap<K, V, K1, V1> : IKTableProcessorSupplier<K, V, KeyValuePair<K1, V1>>
//    {
//        private IKTable<K, V> parent;
//        private IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper;

//        public KTableRepartitionMap(IKTable<K, V> parent, IKeyValueMapper<K, V, KeyValuePair<K1, V1>> mapper)
//        {
//            this.parent = parent;
//            this.mapper = mapper;
//        }

//        public IKeyValueProcessor<K, Change<V>> Get()
//        {
//            return new KTableMapProcessor();
//        }

//        public IKTableValueGetterSupplier<K, KeyValuePair<K1, V1>> View()
//        {
//            //KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.ValueGetterSupplier();

//            return new KTableValueGetterSupplier<K, V, KeyValuePair<K1, V1>>();
//            //{
//            //
//            //public KTableValueGetter<K, KeyValuePair<K1, V1>> get()
//            //{
//            //    return new KTableMapValueGetter(parentValueGetterSupplier.Get());
//            //}
//            //
//            //    public string[] storeNames()
//            //{
//            //    throw new StreamsException("Underlying state store not accessible due to repartitioning.");
//            //}
//            //};
//        }

//        /**
//         * @throws IllegalStateException since this method should never be called
//         */
//        public void EnableSendingOldValues()
//        {
//            // this should never be called
//            throw new InvalidOperationException("KTableRepartitionMap should always require sending old values.");
//        }

//        private class KTableMapProcessor : AbstractProcessor<K, Change<V>>
//        {
//            /**
//             * @throws StreamsException if key is null
//             */
//            public override void Process(K key, Change<V> change)
//            {
//                // the original key should never be null
//                if (key == null)
//                {
//                    throw new StreamsException("Record key for the grouping KTable should not be null.");
//                }

//                // if the value is null, we do not need to forward its selected key-value further
//                KeyValuePair<K1, V1>? newPair = change.newValue == null ? null : mapper.apply(key, change.newValue);
//                KeyValuePair<K1, V1>? oldPair = change.oldValue == null ? null : mapper.apply(key, change.oldValue);

//                // if the selected repartition key or value is null, skip
//                // forward oldPair first, to be consistent with reduce and aggregate
//                if (oldPair != null && oldPair.Key != null && oldPair.Value != null)
//                {
//                    context.Forward(oldPair.Key, new Change<V>(null, oldPair.Value));
//                }

//                if (newPair != null && newPair.Key != null && newPair.Value != null)
//                {
//                    context.Forward(newPair.Key, new Change<V>(newPair.Value));
//                }
//            }
//        }

//        private class KTableMapValueGetter : IKTableValueGetter<K, KeyValuePair<K1, V1>>
//        {
//            private IKTableValueGetter<K, V> parentGetter;
//            private IProcessorContext? context;

//            public KTableMapValueGetter(IKTableValueGetter<K, V> parentGetter)
//            {
//                this.parentGetter = parentGetter;
//            }

//            public void Init(IProcessorContext context, string storeName)
//            {
//                this.context = context;
//                parentGetter.Init(context, storeName);
//            }

//            public ValueAndTimestamp<KeyValuePair<K1, V1>> Get(K key)
//            {
//                ValueAndTimestamp<V> valueAndTimestamp = parentGetter.Get(key);
//                return ValueAndTimestamp.Make(
//                    mapper.apply(key, valueAndTimestamp.value),
//                    valueAndTimestamp == null ? context.timestamp : valueAndTimestamp.timestamp);
//            }

//            public void Close()
//            {
//                parentGetter.Close();
//            }
//        }
//    }
//}
