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
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableSourceValueGetter<K, V> : IKTableValueGetter<K, V>
    {
        private ITimestampedKeyValueStore<K, V> store = null;

        public void init(IProcessorContext context, string storeName)
        {
            store = (ITimestampedKeyValueStore<K, V>)context.getStateStore(storeName);
        }

        public ValueAndTimestamp<V> get(K key)
        {
            return store.get(key);
        }

        public void close() { }

        ValueAndTimestamp<V> IKTableValueGetter<K, V>.get(K key)
        {
            throw new NotImplementedException();
        }
    }
}
