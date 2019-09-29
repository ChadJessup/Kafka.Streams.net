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
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.State;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableFilter<K, S, V> : IKTableProcessorSupplier<K, V, V>
    {
        private readonly KTable<K, S, V> parent;
        private readonly IPredicate<K, V> predicate;
        private readonly bool filterNot;
        private readonly string queryableName;
        private bool sendOldValues = false;

        public KTableFilter(
            KTable<K, S, V> parent,
            IPredicate<K, V> predicate,
            bool filterNot,
            string queryableName)
        {
            this.parent = parent;
            this.predicate = predicate;
            this.filterNot = filterNot;
            this.queryableName = queryableName;
        }


        public IProcessor<K, Change<V>> get()
        {
            return null; // new KTableFilterProcessor();
        }


        public void enableSendingOldValues()
        {
            parent.enableSendingOldValues();
            sendOldValues = true;
        }

        private V computeValue(K key, V value)
        {
            V newValue = default;

            if (value != null && (filterNot))// ^ predicate.test(key, value)))
            {
                newValue = value;
            }

            return newValue;
        }

        private ValueAndTimestamp<V> computeValue(K key, ValueAndTimestamp<V> valueAndTimestamp)
        {
            ValueAndTimestamp<V> newValueAndTimestamp = null;

            if (valueAndTimestamp != null)
            {
                V value = default;// valueAndTimestamp.value();
                if (filterNot)// ^ predicate.test(key, value))
                {
                    newValueAndTimestamp = valueAndTimestamp;
                }
            }

            return newValueAndTimestamp;
        }

        public IKTableValueGetterSupplier<K, V> view()
        {
            // if the KTable is materialized, use the materialized store to return getter value;
            // otherwise rely on the parent getter and apply filter on-the-fly
            if (queryableName != null)
            {
                return new KTableMaterializedValueGetterSupplier<K, V>(queryableName);
            }
            else
            {
                return null;// new KTableValueGetterSupplier<K, V>();
                //{
                //                 KTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

                //            public KTableValueGetter<K, V> get()
                //            {
                //                return new KTableFilterValueGetter(parentValueGetterSupplier());
                //            }


                //            public string[] storeNames()
                //            {
                //                return parentValueGetterSupplier.storeNames();
                //            }
                //        };
            }
        }
    }
}