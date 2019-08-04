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

namespace Kafka.streams.kstream.internals.graph;







public class GlobalStoreNode : StateStoreNode {


    private  string sourceName;
    private  string topic;
    private  ConsumedInternal consumed;
    private  string processorName;
    private  ProcessorSupplier stateUpdateSupplier;


    public GlobalStoreNode( StoreBuilder<IKeyValueStore> storeBuilder,
                            string sourceName,
                            string topic,
                            ConsumedInternal consumed,
                            string processorName,
                            ProcessorSupplier stateUpdateSupplier)
{

        super(storeBuilder);
        this.sourceName = sourceName;
        this.topic = topic;
        this.consumed = consumed;
        this.processorName = processorName;
        this.stateUpdateSupplier = stateUpdateSupplier;
    }


    
    
    public void writeToTopology( InternalTopologyBuilder topologyBuilder)
{
        storeBuilder.withLoggingDisabled();
        topologyBuilder.AddGlobalStore(storeBuilder,
                                       sourceName,
                                       consumed.timestampExtractor(),
                                       consumed.keyDeserializer(),
                                       consumed.valueDeserializer(),
                                       topic,
                                       processorName,
                                       stateUpdateSupplier);

    }


    
    public string ToString()
{
        return "GlobalStoreNode{" +
               "sourceName='" + sourceName + '\'' +
               ", topic='" + topic + '\'' +
               ", processorName='" + processorName + '\'' +
               "} ";
    }
}
