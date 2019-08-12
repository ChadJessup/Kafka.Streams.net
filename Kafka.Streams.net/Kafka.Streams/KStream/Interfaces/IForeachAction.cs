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
namespace Kafka.Streams.KStream.Interfaces
{
    /**
     * The {@code IForeachAction} interface for performing an action on a {@link org.apache.kafka.streams.KeyValue key-value
     * pair}.
     * This is a stateless record-by-record operation, i.e, {@link #apply(object, object)} is invoked individually for each
     * record of a stream.
     * If stateful processing is required, consider using
     * {@link KStream#process(org.apache.kafka.streams.processor.IProcessorSupplier, string...) KStream#process(...)}.
     *
     * @param key type
     * @param value type
     * @see KStream#foreach(IForeachAction)
     */
    public interface IForeachAction<K, V>
    {
        /**
         * Perform an action for each record of a stream.
         *
         * @param key   the key of the record
         * @param value the value of the record
         */
        void apply(K key, V value);
    }
}
