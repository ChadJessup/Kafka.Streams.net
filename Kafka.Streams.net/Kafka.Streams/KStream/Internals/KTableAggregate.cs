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
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor;

namespace Kafka.Streams.KStream.Internals
{
    public class KTableAggregate<K, V, T> : IKTableProcessorSupplier<K, V, T>
    {

        private string storeName;
        private IInitializer<T> initializer;
        private IAggregator<K, V, T> add;
        private IAggregator<K, V, T> Remove;

        private bool sendOldValues = false;

        KTableAggregate(string storeName,
                         IInitializer<T> initializer,
                         IAggregator<K, V, T> add,
                         IAggregator<K, V, T> Remove)
        {
            this.storeName = storeName;
            this.initializer = initializer;
            this.add = add;
            this.Remove = Remove;
        }


        public void enableSendingOldValues()
        {
            sendOldValues = true;
        }


        public IProcessor<K, Change<V>> get()
        {
            return new KTableAggregateProcessor();
        }


        public IKTableValueGetterSupplier<K, T> view()
        {
            return new KTableMaterializedValueGetterSupplier<>(storeName);
        }
    }
}