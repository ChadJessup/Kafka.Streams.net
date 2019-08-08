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

namespace Kafka.Streams.KStream.Internals.Graph
{
    public partial class TableSourceNode<K, V>
    {
        public static class TableSourceNodeBuilder<K, V>
        {

            private string nodeName;
            private string sourceName;
            private string topic;
            private ConsumedInternal<K, V> consumedInternal;
            private MaterializedInternal<K, V, object> materializedInternal;
            private ProcessorParameters<K, V> processorParameters;
            private bool isGlobalKTable = false;

            private TableSourceNodeBuilder()
            {
            }

            public TableSourceNodeBuilder<K, V> withSourceName(string sourceName)
            {
                this.sourceName = sourceName;
                return this;
            }

            public TableSourceNodeBuilder<K, V> withTopic(string topic)
            {
                this.topic = topic;
                return this;
            }

            public TableSourceNodeBuilder<K, V> withMaterializedInternal(MaterializedInternal<K, V, object> materializedInternal)
            {
                this.materializedInternal = materializedInternal;
                return this;
            }

            public TableSourceNodeBuilder<K, V> withConsumedInternal(ConsumedInternal<K, V> consumedInternal)
            {
                this.consumedInternal = consumedInternal;
                return this;
            }

            public TableSourceNodeBuilder<K, V> withProcessorParameters(ProcessorParameters<K, V> processorParameters)
            {
                this.processorParameters = processorParameters;
                return this;
            }

            public TableSourceNodeBuilder<K, V> withNodeName(string nodeName)
            {
                this.nodeName = nodeName;
                return this;
            }

            public TableSourceNodeBuilder<K, V> isGlobalKTable(bool isGlobaKTable)
            {
                this.isGlobalKTable = isGlobaKTable;
                return this;
            }

            public TableSourceNode<K, V> build()
            {
                return new TableSourceNode<>(nodeName,
                                             sourceName,
                                             topic,
                                             consumedInternal,
                                             materializedInternal,
                                             processorParameters,
                                             isGlobalKTable);
            }
        }
    }
