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

//using Kafka.Common.Utils;
//using Kafka.Streams.Processor.Interfaces;
//using Kafka.Streams.Processor.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Internals;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals.Graph
//{
//    /**
//     * Used to represent either a KTable source or a GlobalKTable source. A bool flag is used to indicate if this represents a GlobalKTable a {@link
//     * org.apache.kafka.streams.kstream.GlobalKTable}
//     */
//    public class TableSourceNode<K, V, T> : StreamSourceNode<K, V>
//        where T : IStateStore
//    {
//        private MaterializedInternal<K, V, T> materializedInternal;
//        private ProcessorParameters<K, V> processorParameters;
//        private string sourceName;
//        private bool isGlobalKTable;
//        private bool shouldReuseSourceTopicForChangelog = false;

//        private TableSourceNode(
//            string nodeName,
//            string sourceName,
//            string topic,
//            ConsumedInternal<K, V> consumedInternal,
//            MaterializedInternal<K, V, T> materializedInternal,
//            ProcessorParameters<K, V> processorParameters,
//            bool isGlobalKTable)
//            : base(nodeName,
//                  new List<string> { topic },
//                  consumedInternal)
//        {

//            this.sourceName = sourceName;
//            this.isGlobalKTable = isGlobalKTable;
//            this.processorParameters = processorParameters;
//            this.materializedInternal = materializedInternal;
//        }

//        public void reuseSourceTopicForChangeLog(bool shouldReuseSourceTopicForChangelog)
//        {
//            this.shouldReuseSourceTopicForChangelog = shouldReuseSourceTopicForChangelog;
//        }

//        public override string ToString()
//        {
//            return "TableSourceNode{" +
//                   "materializedInternal=" + materializedInternal +
//                   ", processorParameters=" + processorParameters +
//                   ", sourceName='" + sourceName + '\'' +
//                   ", isGlobalKTable=" + isGlobalKTable +
//                   "} " + base.ToString();
//        }

//        public static TableSourceNodeBuilder<K, V, T> tableSourceNodeBuilder()
//        {
//            return new TableSourceNodeBuilder<K, V, T>();
//        }



//        public override void writeToTopology(InternalTopologyBuilder topologyBuilder)
//        {
//            string topicName = getTopicNames().GetEnumerator().Current;

//            // TODO: we assume source KTables can only be timestamped-key-value stores for now.
//            // should be expanded for other types of stores as well.
//            IStoreBuilder<ITimestampedKeyValueStore<K, V>> storeBuilder =
//               new TimestampedKeyValueStoreMaterializer<K, V>(
//                   (MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>)materializedInternal).materialize();

//            if (isGlobalKTable)
//            {
//                topologyBuilder.addGlobalStore(
//                    storeBuilder,
//                    sourceName,
//                    consumedInternal.timestampExtractor,
//                    consumedInternal.keyDeserializer(),
//                    consumedInternal.valueDeserializer(),
//                    topicName,
//                    processorParameters.processorName,
//                    processorParameters.IProcessorSupplier);
//            }
//            else
//            {

//                topologyBuilder.addSource(
//                    consumedInternal.offsetResetPolicy(),
//                    sourceName,
//                    consumedInternal.timestampExtractor,
//                    consumedInternal.keyDeserializer(),
//                    consumedInternal.valueDeserializer(),
//                    new[] { topicName });

//                topologyBuilder.addProcessor(processorParameters.processorName, processorParameters.IProcessorSupplier, sourceName);

//                // only add state store if the source KTable should be materialized
//                KTableSource<K, V> ktableSource = (KTableSource<K, V>)processorParameters.IProcessorSupplier;
//                if (ktableSource.queryableName() != null)
//                {
//                    topologyBuilder.addStateStore<K, V, ITimestampedKeyValueStore<K, V>>(storeBuilder, new[] { nodeName });

//                    if (shouldReuseSourceTopicForChangelog)
//                    {
//                        storeBuilder.withLoggingDisabled();
//                        topologyBuilder.connectSourceStoreAndTopic(storeBuilder.name, topicName);
//                    }
//                }
//            }

//        }
//    }
//}