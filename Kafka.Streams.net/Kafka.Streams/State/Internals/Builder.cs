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
//using Kafka.Streams.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    public partial class InMemoryTimeOrderedKeyValueBuffer<K, V>
//    {
//        public static class Builder<K, V> : IStoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>>
//        {

//            private string storeName;
//            private ISerde<K> keySerde;
//            private ISerde<V> valSerde;
//            private bool loggingEnabled = true;

//            public Builder(string storeName, ISerde<K> keySerde, ISerde<V> valSerde)
//            {
//                this.storeName = storeName;
//                this.keySerde = keySerde;
//                this.valSerde = valSerde;
//            }

//            /**
//             * As of 2.1, there's no way for users to directly interact with the buffer,
//             * so this method is implemented solely to be called by Streams (which
//             * it will do based on the {@code cache.max.bytes.buffering} config.
//             * <p>
//             * It's currently a no-op.
//             */

//            public IStoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> withCachingEnabled()
//            {
//                return this;
//            }

//            /**
//             * As of 2.1, there's no way for users to directly interact with the buffer,
//             * so this method is implemented solely to be called by Streams (which
//             * it will do based on the {@code cache.max.bytes.buffering} config.
//             * <p>
//             * It's currently a no-op.
//             */

//            public IStoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> withCachingDisabled()
//            {
//                return this;
//            }


//            public IStoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> withLoggingEnabled(Dictionary<string, string> config)
//            {
//                throw new InvalidOperationException();
//            }


//            public IStoreBuilder<InMemoryTimeOrderedKeyValueBuffer<K, V>> withLoggingDisabled()
//            {
//                loggingEnabled = false;
//                return this;
//            }


//            public InMemoryTimeOrderedKeyValueBuffer<K, V> build()
//            {
//                return new InMemoryTimeOrderedKeyValueBuffer<>(storeName, loggingEnabled, keySerde, valSerde);
//            }


//            public Dictionary<string, string> logConfig()
//            {
//                return Collections.emptyMap();
//            }


//            public bool loggingEnabled { get; }
//            public string name => storeName;
//        }
//    }
//}
