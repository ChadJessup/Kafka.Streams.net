/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace Kafka.Streams.KStream.Internals
{
    /**
     * KTable repartition map functions are not exposed to public APIs, but only used for keyed aggregations.
     * <p>
     * Given the input, it can output at most two records (one mapped from old value and one mapped from new value).
     */
    public class KTableRepartitionMap<K, V, K1, V1> : IKTableProcessorSupplier<K, V, KeyValue<K1, V1>>
    {

        private KTableImpl<K, object, V> parent;
        private IKeyValueMapper<K, V, KeyValue<K1, V1>> mapper;

        KTableRepartitionMap(KTableImpl<K, ?, V> parent, IKeyValueMapper<K, V, KeyValue<K1, V1>> mapper)
        {
            this.parent = parent;
            this.mapper = mapper;
        }


        public Processor<K, Change<V>> get()
        {
            return new KTableMapProcessor();
        }


        public KTableValueGetterSupplier<K, KeyValue<K1, V1>> view()
        {
            KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

            return new KTableValueGetterSupplier<K, KeyValue<K1, V1>>()
        //    {

            //    public KTableValueGetter<K, KeyValue<K1, V1>> get()
            //    {
            //        return new KTableMapValueGetter(parentValueGetterSupplier());
            //    }


            //    public string[] storeNames()
            //    {
            //        throw new StreamsException("Underlying state store not accessible due to repartitioning.");
            //    }
            //};
        }

        /**
         * @throws InvalidOperationException since this method should never be called
         */

        public void enableSendingOldValues()
        {
            // this should never be called
            throw new InvalidOperationException("KTableRepartitionMap should always require sending old values.");
        }

        private class KTableMapProcessor : AbstractProcessor<K, Change<V>>
        {

            /**
             * @throws StreamsException if key is null
             */

            public void process(K key, Change<V> change)
            {
                // the original key should never be null
                if (key == null)
                {
                    throw new StreamsException("Record key for the grouping KTable should not be null.");
                }

                // if the value is null, we do not need to forward its selected key-value further
                KeyValue<K1, V1> newPair = change.newValue == null ? null : mapper.apply(key, change.newValue);
                KeyValue<K1, V1> oldPair = change.oldValue == null ? null : mapper.apply(key, change.oldValue);

                // if the selected repartition key or value is null, skip
                // forward oldPair first, to be consistent with reduce and aggregate
                if (oldPair != null && oldPair.key != null && oldPair.value != null)
                {
                    context().forward(oldPair.key, new Change<>(null, oldPair.value));
                }

                if (newPair != null && newPair.key != null && newPair.value != null)
                {
                    context().forward(newPair.key, new Change<>(newPair.value, null));
                }

            }
        }

        private class KTableMapValueGetter : KTableValueGetter<K, KeyValue<K1, V1>>
        {

            private KTableValueGetter<K, V> parentGetter;
            private IProcessorContext context;

            KTableMapValueGetter(KTableValueGetter<K, V> parentGetter)
            {
                this.parentGetter = parentGetter;
            }


            public void init(IProcessorContext context)
            {
                this.context = context;
                parentGetter.init(context);
            }


            public ValueAndTimestamp<KeyValue<K1, V1>> get(K key)
            {
                ValueAndTimestamp<V> valueAndTimestamp = parentGetter[key];
                return ValueAndTimestamp.make(
                    mapper.apply(key, getValueOrNull(valueAndTimestamp)),
                    valueAndTimestamp == null ? context.timestamp() : valueAndTimestamp.timestamp());
            }


            public void close()
            {
                parentGetter.close();
            }
        }
    }
}