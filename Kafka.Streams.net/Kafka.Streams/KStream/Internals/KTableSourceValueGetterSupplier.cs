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






public class KTableSourceValueGetterSupplier<K, V> : IKTableValueGetterSupplier<K, V> {
    private string storeName;

    KTableSourceValueGetterSupplier(string storeName)
    {
        this.storeName = storeName;
    }

    public IKTableValueGetter<K, V> get()
    {
        return new KTableSourceValueGetter();
    }


    public string[] storeNames()
    {
        return new string[] { storeName };
    }

    private class KTableSourceValueGetter : IKTableValueGetter<K, V>
    {
        private ITimestampedKeyValueStore<K, V> store = null;


        public void init(IProcessorContext<K, V> context)
        {
            store = (TimestampedKeyValueStore<K, V>)context.getStateStore(storeName);
        }

        public ValueAndTimestamp<V> get(K key)
        {
            return store[key];
        }


        public void close() { }
    }
}
