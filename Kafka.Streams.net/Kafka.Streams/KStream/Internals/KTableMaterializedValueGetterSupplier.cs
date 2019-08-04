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
namespace Kafka.Streams.KStream.Internals {





public KTableMaterializedValueGetterSupplier<K, V> : KTableValueGetterSupplier<K, V> {
    private  string storeName;

    KTableMaterializedValueGetterSupplier( string storeName)
{
        this.storeName = storeName;
    }

    public KTableValueGetter<K, V> get()
{
        return new KTableMaterializedValueGetter();
    }

    
    public string[] storeNames()
{
        return new string[]{storeName};
    }

    private KTableMaterializedValueGetter : KTableValueGetter<K, V> {
        private TimestampedKeyValueStore<K, V> store;

        
        
        public void init( IProcessorContext context)
{
            store = (TimestampedKeyValueStore<K, V>) context.getStateStore(storeName);
        }

        
        public ValueAndTimestamp<V> get( K key)
{
            return store[key];
        }

        
        public void close() {}
    }
}
