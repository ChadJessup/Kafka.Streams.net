///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for.Additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//namespace Kafka.Streams.State.Internals
//{
//    public class GlobalStateStoreProvider : IStateStoreProvider
//    {
//        private Dictionary<string, IStateStore> globalStateStores;

//        public GlobalStateStoreProvider(Dictionary<string, IStateStore> globalStateStores)
//        {
//            this.globalStateStores = globalStateStores;
//        }


//        public override List<T> stores(string storeName, IQueryableStoreType<T> queryableStoreType)
//        {
//            IStateStore store = globalStateStores[storeName];
//            if (store == null || !queryableStoreType.accepts(store))
//            {
//                return Collections.emptyList();
//            }
//            if (!store.isOpen())
//            {
//                throw new InvalidStateStoreException("the state store, " + storeName + ", is not open.");
//            }
//            if (store is ITimestampedKeyValueStore && queryableStoreType is QueryableStoreTypes.KeyValueStoreType)
//            {
//                return (List<T>)Collections.singletonList(new ReadOnlyKeyValueStoreFacade((ITimestampedKeyValueStore<object, object>)store));
//            }
//            else if (store is ITimestampedWindowStore && queryableStoreType is QueryableStoreTypes.WindowStoreType)
//            {
//                return (List<T>)Collections.singletonList(new ReadOnlyWindowStoreFacade((ITimestampedWindowStore<object, object>)store));
//            }
//            return (List<T>)Collections.singletonList(store);
//        }
//    }
//}