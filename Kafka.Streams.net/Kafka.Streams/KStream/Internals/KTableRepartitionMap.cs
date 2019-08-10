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
using Kafka.Streams.IProcessor;
using Kafka.Streams.IProcessor.Interfaces;
using System;

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

        KTableRepartitionMap(KTableImpl<K, object, V> parent, IKeyValueMapper<K, V, KeyValue<K1, V1>> mapper)
        {
            this.parent = parent;
            this.mapper = mapper;
        }

        public IProcessor<K, Change<V>> get()
        {
            return new KTableMapProcessor();
        }

        public IKTableValueGetterSupplier<K, KeyValue<K1, V1>> view()
        {
            IKTableValueGetterSupplier<K, V> parentValueGetterSupplier = parent.valueGetterSupplier();

            return new KTableValueGetterSupplier<K, KeyValue<K1, V1>>();
        }

        /**
         * @throws InvalidOperationException since this method should never be called
         */

        public void enableSendingOldValues()
        {
            // this should never be called
            throw new InvalidOperationException("KTableRepartitionMap should always require sending old values.");
        }
    }
}