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
     * The {@code Reducer} interface for combining two values of the same type into a new value.
     * In contrast to {@link IAggregator} the result type must be the same as the input type.
     * <p>
     * The provided values can be either original values from input {@link KeyValue} pair records or be a previously
     * computed result from {@link Reducer#apply(object, object)}.
     * <p>
     * {@code Reducer} can be used to implement aggregation functions like sum, min, or max.
     *
     * @param value type
     * @see KGroupedStream#reduce(Reducer)
     * @see KGroupedStream#reduce(Reducer, Materialized)
     * @see TimeWindowedKStream#reduce(Reducer)
     * @see TimeWindowedKStream#reduce(Reducer, Materialized)
     * @see SessionWindowedKStream#reduce(Reducer)
     * @see SessionWindowedKStream#reduce(Reducer, Materialized)
     * @see IAggregator
     */
    public interface IReducer<V>
    {
        /**
         * Aggregate the two given values into a single one.
         *
         * @param value1 the first value for the aggregation
         * @param value2 the second value for the aggregation
         * @return the aggregated value
         */
        V apply(V value1, V value2);
    }
}