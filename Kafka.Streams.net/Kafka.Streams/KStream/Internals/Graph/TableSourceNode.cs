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














/**
 * Used to represent either a KTable source or a GlobalKTable source. A bool flag is used to indicate if this represents a GlobalKTable a {@link
 * org.apache.kafka.streams.kstream.GlobalKTable}
 */
public TableSourceNode<K, V> : StreamSourceNode<K, V> {

    private  MaterializedInternal<K, V, ?> materializedInternal;
    private  ProcessorParameters<K, V> processorParameters;
    private  string sourceName;
    private  bool isGlobalKTable;
    private bool shouldReuseSourceTopicForChangelog = false;

    private TableSourceNode( string nodeName,
                             string sourceName,
                             string topic,
                             ConsumedInternal<K, V> consumedInternal,
                             MaterializedInternal<K, V, ?> materializedInternal,
                             ProcessorParameters<K, V> processorParameters,
                             bool isGlobalKTable)
{

        base(nodeName,
              Collections.singletonList(topic),
              consumedInternal);

        this.sourceName = sourceName;
        this.isGlobalKTable = isGlobalKTable;
        this.processorParameters = processorParameters;
        this.materializedInternal = materializedInternal;
    }


    public void reuseSourceTopicForChangeLog( bool shouldReuseSourceTopicForChangelog)
{
        this.shouldReuseSourceTopicForChangelog = shouldReuseSourceTopicForChangelog;
    }

    
    public string ToString()
{
        return "TableSourceNode{" +
               "materializedInternal=" + materializedInternal +
               ", processorParameters=" + processorParameters +
               ", sourceName='" + sourceName + '\'' +
               ", isGlobalKTable=" + isGlobalKTable +
               "} " + base.ToString();
    }

    public staticTableSourceNodeBuilder<K, V> tableSourceNodeBuilder()
{
        return new TableSourceNodeBuilder<>();
    }

    
    
    public void writeToTopology( InternalTopologyBuilder topologyBuilder)
{
         string topicName = getTopicNames().iterator().next();

        // TODO: we assume source KTables can only be timestamped-key-value stores for now.
        // should be expanded for other types of stores as well.
         StoreBuilder<TimestampedKeyValueStore<K, V>> storeBuilder =
            new TimestampedKeyValueStoreMaterializer<>((MaterializedInternal<K, V, IKeyValueStore<Bytes, byte[]>>) materializedInternal).materialize();

        if (isGlobalKTable)
{
            topologyBuilder.AddGlobalStore(storeBuilder,
                                           sourceName,
                                           consumedInternal().timestampExtractor(),
                                           consumedInternal().keyDeserializer(),
                                           consumedInternal().valueDeserializer(),
                                           topicName,
                                           processorParameters.processorName(),
                                           processorParameters.processorSupplier());
        } else
{

            topologyBuilder.AddSource(consumedInternal().offsetResetPolicy(),
                                      sourceName,
                                      consumedInternal().timestampExtractor(),
                                      consumedInternal().keyDeserializer(),
                                      consumedInternal().valueDeserializer(),
                                      topicName);

            topologyBuilder.AddProcessor(processorParameters.processorName(), processorParameters.processorSupplier(), sourceName);

            // only.Add state store if the source KTable should be materialized
             KTableSource<K, V> ktableSource = (KTableSource<K, V>) processorParameters.processorSupplier();
            if (ktableSource.queryableName() != null)
{
                topologyBuilder.AddStateStore(storeBuilder, nodeName());

                if (shouldReuseSourceTopicForChangelog)
{
                    storeBuilder.withLoggingDisabled();
                    topologyBuilder.connectSourceStoreAndTopic(storeBuilder.name(), topicName);
                }
            }
        }

    }

    public static  TableSourceNodeBuilder<K, V> {

        private string nodeName;
        private string sourceName;
        private string topic;
        private ConsumedInternal<K, V> consumedInternal;
        private MaterializedInternal<K, V, ?> materializedInternal;
        private ProcessorParameters<K, V> processorParameters;
        private bool isGlobalKTable = false;

        private TableSourceNodeBuilder()
{
        }

        public TableSourceNodeBuilder<K, V> withSourceName( string sourceName)
{
            this.sourceName = sourceName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withTopic( string topic)
{
            this.topic = topic;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withMaterializedInternal( MaterializedInternal<K, V, ?> materializedInternal)
{
            this.materializedInternal = materializedInternal;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withConsumedInternal( ConsumedInternal<K, V> consumedInternal)
{
            this.consumedInternal = consumedInternal;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withProcessorParameters( ProcessorParameters<K, V> processorParameters)
{
            this.processorParameters = processorParameters;
            return this;
        }

        public TableSourceNodeBuilder<K, V> withNodeName( string nodeName)
{
            this.nodeName = nodeName;
            return this;
        }

        public TableSourceNodeBuilder<K, V> isGlobalKTable( bool isGlobaKTable)
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
