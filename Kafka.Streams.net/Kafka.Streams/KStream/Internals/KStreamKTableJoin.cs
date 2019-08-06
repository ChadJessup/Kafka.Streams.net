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
using Kafka.Streams.Processor;

namespace Kafka.Streams.KStream.Internals
{
    class KStreamKTableJoin<K, R, V1, V2> : IProcessorSupplier<K, V1>
    {

        //    private IKeyValueMapper<K, V1, K> keyValueMapper = new IKeyValueMapper<K, V1, K>()
        //    {
        //    public K apply(K key, V1 value)
        //    {
        //        return key;
        //    }
        //};

        private KTableValueGetterSupplier<K, V2> valueGetterSupplier;
        private ValueJoiner<V1, V2, R> joiner;
        private bool leftJoin;

        KStreamKTableJoin(KTableValueGetterSupplier<K, V2> valueGetterSupplier, ValueJoiner<V1, V2, R> joiner, bool leftJoin)
        {
            this.valueGetterSupplier = valueGetterSupplier;
            this.joiner = joiner;
            this.leftJoin = leftJoin;
        }


        public Processor<K, V1> get()
        {
            return null; // new KStreamKTableJoinProcessor<>(valueGetterSupplier(), keyValueMapper, joiner, leftJoin);
        }

    }
}