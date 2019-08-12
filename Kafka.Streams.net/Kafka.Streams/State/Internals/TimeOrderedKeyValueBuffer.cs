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
//using Confluent.Kafka;
//using Kafka.Common.Interfaces;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processor.Interfaces;
//using Kafka.Streams.Processor.Internals;

//namespace Kafka.Streams.State.Internals
//{
//    public interface TimeOrderedKeyValueBuffer<K, V> : IStateStore
//    {

//        void setSerdesIfNull(ISerde<K> keySerde, ISerde<V> valueSerde);

//        void evictWhile(ISupplier<bool> predicate, IConsumer<K, Eviction<K, V>> callback);

//        Maybe<ValueAndTimestamp<V>> priorValueForBuffered(K key);

//        void put(long time, K key, Change<V> value, ProcessorRecordContext recordContext);

//        int numRecords();

//        long bufferSize();

//        long minTimestamp();
//    }
//}