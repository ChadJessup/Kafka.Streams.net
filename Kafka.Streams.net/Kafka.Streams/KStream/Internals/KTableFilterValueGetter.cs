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
    public class KTableFilterValueGetter<K, V> : IKTableValueGetter<K, V>
    {
        private IKTableValueGetter<K, V> parentGetter;

        KTableFilterValueGetter(IKTableValueGetter<K, V> parentGetter)
        {
            this.parentGetter = parentGetter;
        }



        public void init(IProcessorContext<K, V> context)
        {
            parentGetter.init(context);
        }


        public ValueAndTimestamp<V> get(K key)
        {
            return null; // computeValue(key, parentGetter[key]);
        }


        public void close()
        {
            parentGetter.close();
        }
    }
}