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
namespace Kafka.Streams.Processors.Interfaces
{
    /// <summary>
    /// An interface that dynamically determines the name of the Kafka topic to send at the sink node of the topology.
    /// </summary>
    public interface ITopicNameExtractor
    {
        /// <summary>
        /// Extracts the topic name to send to.The topic name must already exist, since the Kafka Streams library will not
        /// try to automatically create the topic with the extracted name.
        /// </summary>
        /// <param name="key">The record key.</param>
        /// <param name="value">The record value.</param>
        /// <param name="recordContext">Current context metadata of the record.</param>
        /// <returns>The topic name this record should be sent to.</returns>
        string Extract<K, V>(K key, V value, IRecordContext recordContext);
    }
}