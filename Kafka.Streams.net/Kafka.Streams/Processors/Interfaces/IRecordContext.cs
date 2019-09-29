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
using Confluent.Kafka;

namespace Kafka.Streams.Processors
{
    /**
     * The context associated with the current record being processed by
     * an {@link IProcessor}
     */
    public interface IRecordContext
    {
        /**
         * @return  The offset of the original record received from Kafka;
         *          could be -1 if it is not available
         */
        long offset { get; }

        /**
         * @return  The timestamp extracted from the record received from Kafka;
         *          could be -1 if it is not available
         */
        long timestamp { get; }

        /**
         * @return  The topic the record was received on;
         *          could be null if it is not available
         */
        string Topic { get; }

        /**
         * @return  The partition the record was received on;
         *          could be -1 if it is not available
         */
        int partition { get; }

        /**
         * @return  The headers from the record received from Kafka;
         *          could be null if it is not available
         */
        Headers headers { get; }
    }
}