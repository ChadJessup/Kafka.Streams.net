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





public abstract KTableKTableAbstractJoinValueGetterSupplier<K, R, V1, V2> : KTableValueGetterSupplier<K, R> {
     KTableValueGetterSupplier<K, V1> valueGetterSupplier1;
     KTableValueGetterSupplier<K, V2> valueGetterSupplier2;

    KTableKTableAbstractJoinValueGetterSupplier( KTableValueGetterSupplier<K, V1> valueGetterSupplier1,
                                                 KTableValueGetterSupplier<K, V2> valueGetterSupplier2)
{
        this.valueGetterSupplier1 = valueGetterSupplier1;
        this.valueGetterSupplier2 = valueGetterSupplier2;
    }

    
    public string[] storeNames()
{
         string[] storeNames1 = valueGetterSupplier1.storeNames();
         string[] storeNames2 = valueGetterSupplier2.storeNames();
         HashSet<string> stores = new HashSet<>(storeNames1.Length + storeNames2.Length);
        Collections.AddAll(stores, storeNames1);
        Collections.AddAll(stores, storeNames2);
        return stores.toArray(new string[stores.size()]);
    }

}
