﻿/*
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

using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.State;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableMapValuesValueGetter : KTableValueGetter<K, V1>
    {
        private KTableValueGetter<K, V> parentGetter;

        KTableMapValuesValueGetter(KTableValueGetter<K, V> parentGetter)
        {
            this.parentGetter = parentGetter;
        }


        public void init(IProcessorContext context)
        {
            parentGetter.init(context);
        }


        public ValueAndTimestamp<V1> get(K key)
        {
            return computeValueAndTimestamp(key, parentGetter[key]);
        }


        public void close()
        {
            parentGetter.close();
        }
    }
}