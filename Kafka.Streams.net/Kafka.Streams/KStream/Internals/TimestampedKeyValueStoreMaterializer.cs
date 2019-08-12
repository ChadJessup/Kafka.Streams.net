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
//using Kafka.Common.Utils;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Internals;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class TimestampedKeyValueStoreMaterializer<K, V>
//    {
//        private MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized;

//        public TimestampedKeyValueStoreMaterializer(
//            MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>> materialized)
//        {
//            this.materialized = materialized;
//        }

//        /**
//         * @return  StoreBuilder
//         */
//        public IStoreBuilder<ITimestampedKeyValueStore<K, V>> materialize()
//        {
//            IKeyValueBytesStoreSupplier supplier = (IKeyValueBytesStoreSupplier)materialized.storeSupplier;
//            if (supplier == null)
//            {
//                string name = materialized.storeName();
//                //                supplier = Stores.persistentTimestampedKeyValueStore(name);
//            }

//            IStoreBuilder<ITimestampedKeyValueStore<K, V>> builder = null;
//            //    Stores.timestampedKeyValueStoreBuilder(
//            //   supplier,
//            //   materialized.keySerde,
//            //   materialized.valueSerde);

//            //if (materialized.loggingEnabled)
//            //{
//            //    builder.withLoggingEnabled(materialized.logConfig());
//            //}
//            //else
//            //{

//            //    builder.withLoggingDisabled();
//            //}

//            //if (materialized.cachingEnabled())
//            //{
//            //    builder.withCachingEnabled();
//            //}
//            return builder;
//        }
//    }
//}