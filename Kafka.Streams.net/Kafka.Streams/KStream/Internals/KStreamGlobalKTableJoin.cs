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
    class KStreamGlobalKTableJoin<K1, K2, R, V1, V2> : IProcessorSupplier<K1, V1>
    {

        private KTableValueGetterSupplier<K2, V2> valueGetterSupplier;
        private IValueJoiner<V1, V2, R> joiner;
        private IKeyValueMapper<K1, V1, K2> mapper;
        private bool leftJoin;

        KStreamGlobalKTableJoin(KTableValueGetterSupplier<K2, V2> valueGetterSupplier,
                                 IValueJoiner<V1, V2, R> joiner,
                                 IKeyValueMapper<K1, V1, K2> mapper,
                                 bool leftJoin)
        {
            this.valueGetterSupplier = valueGetterSupplier;
            this.joiner = joiner;
            this.mapper = mapper;
            this.leftJoin = leftJoin;
        }


        public IProcessor<K1, V1> get()
        {
            return new KStreamKTableJoinProcessor<>(valueGetterSupplier(), mapper, joiner, leftJoin);
        }
    }
}
