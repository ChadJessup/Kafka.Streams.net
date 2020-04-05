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
     * The {@code Initializer} interface for creating an initial value in aggregations.
     * {@code Initializer} is used in combination with {@link IAggregator}.
     *
     * @param aggregate value type
     * @see IAggregator
     * @see KGroupedStream#aggregate(Initializer, IAggregator)
     * @see KGroupedStream#aggregate(Initializer, IAggregator, Materialized)
     * @see TimeWindowedKStream#aggregate(Initializer, IAggregator)
     * @see TimeWindowedKStream#aggregate(Initializer, IAggregator, Materialized)
     * @see SessionWindowedKStream#aggregate(Initializer, IAggregator, Merger)
     * @see SessionWindowedKStream#aggregate(Initializer, IAggregator, Merger, Materialized)
     */
    public interface IInitializer<VA>
    {
        /**
         * Return the initial value for an aggregation.
         *
         * @return the initial value for an aggregation
         */
        VA Apply();
    }
}
