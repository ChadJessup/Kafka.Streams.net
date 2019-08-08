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
namespace Kafka.Streams.KStream
{
    /**
     * A {@code TransformerSupplier} interface which can create one or more {@link Transformer} instances.
     *
     * @param key type
     * @param value type
     * @param {@link org.apache.kafka.streams.KeyValue KeyValue} return type (both key and value type can be set
     *            arbitrarily)
     * @see Transformer
     * @see KStream#transform(TransformerSupplier, string...)
     * @see ValueTransformer
     * @see ValueTransformerSupplier
     * @see KStream#transformValues(ValueTransformerSupplier, string...)
     */
    public interface ITransformerSupplier<K, V, R>
    {
        /**
         * Return a new {@link Transformer} instance.
         *
         * @return a new {@link Transformer} instance
         */
        ITransformer<K, V, R> get();
    }
}