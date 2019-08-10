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
using Kafka.Streams.IProcessor.Interfaces;

namespace Kafka.Streams.KStream
{
    /**
     * The {@code Transformer} interface is for stateful mapping of an input record to zero, one, or multiple new output
     * records (both key and value type can be altered arbitrarily).
     * This is a stateful record-by-record operation, i.e, {@link #transform(object, object)} is invoked individually for
     * each record of a stream and can access and modify a state that is available beyond a single call of
     * {@link #transform(object, object)} (cf. {@link KeyValueMapper} for stateless record transformation).
     * Additionally, this {@code Transformer} can {@link IProcessorContext#schedule(Duration, PunctuationType, Punctuator) schedule}
     * a method to be {@link Punctuator#punctuate(long) called periodically} with the provided context.
     * <p>
     * Use {@link TransformerSupplier} to provide new instances of {@code Transformer} to Kafka Stream's runtime.
     * <p>
     * If only a record's value should be modified {@link ValueTransformer} can be used.
     *
     * @param key type
     * @param value type
     * @param {@link KeyValue} return type (both key and value type can be set
     *            arbitrarily)
     * @see TransformerSupplier
     * @see KStream#transform(TransformerSupplier, string...)
     * @see ValueTransformer
     * @see KStream#map(KeyValueMapper)
     * @see KStream#flatMap(KeyValueMapper)
     */
    public interface ITransformer<K, V, R>
    {
        /**
         * Initialize this transformer.
         * This is called once per instance when the topology gets initialized.
         * When the framework is done with the transformer, {@link #close()} will be called on it; the
         * framework may later re-use the transformer by calling {@link #init(IProcessorContext)} again.
         * <p>
         * The provided {@link IProcessorContext<K, V> context} can be used to access topology and record meta data, to
         * {@link IProcessorContext#schedule(Duration, PunctuationType, Punctuator) schedule} a method to be
         * {@link Punctuator#punctuate(long) called periodically} and to access attached {@link IStateStore}s.
         * <p>
         * Note, that {@link IProcessorContext} is updated in the background with the current record's meta data.
         * Thus, it only contains valid record meta data when accessed within {@link #transform(object, object)}.
         *
         * @param context the context
         */
        void init(IProcessorContext<K, V> context);

        /**
         * Transform the record with the given key and value.
         * Additionally, any {@link IStateStore state} that is {@link KStream#transform(TransformerSupplier, string...)
         * attached} to this operator can be accessed and modified
         * arbitrarily (cf. {@link IProcessorContext#getStateStore(string)}).
         * <p>
         * If only one record should be forward downstream, {@code transform} can return a new {@link KeyValue}. If
         * more than one output record should be forwarded downstream, {@link IProcessorContext#forward(object, object)}
         * and {@link IProcessorContext#forward(object, object, To)} can be used.
         * If no record should be forwarded downstream, {@code transform} can return {@code null}.
         *
         * Note that returning a new {@link KeyValue} is merely for convenience. The same can be achieved by using
         * {@link IProcessorContext#forward(object, object)} and returning {@code null}.
         *
         * @param key the key for the record
         * @param value the value for the record
         * @return new {@link KeyValue} pair&mdash;if {@code null} no key-value pair will
         * be forwarded to down stream
         */
        R transform(K key, V value);

        /**
         * Close this transformer and clean up any resources. The framework may
         * later re-use this transformer by calling {@link #init(IProcessorContext)} on it again.
         * <p>
         * To generate new {@link KeyValue} pairs {@link IProcessorContext#forward(object, object)} and
         * {@link IProcessorContext#forward(object, object, To)} can be used.
         */
        void close();
    }
}
