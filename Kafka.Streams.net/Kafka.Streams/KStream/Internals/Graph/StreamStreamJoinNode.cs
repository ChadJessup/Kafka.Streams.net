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







/**
 * Too much information to generalize, so Stream-Stream joins are represented by a specific node.
 */
public class StreamStreamJoinNode<K, V1, V2, VR> : BaseJoinProcessorNode<K, V1, V2, VR> {

    private  ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters;
    private  ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters;
    private  StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder;
    private  StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder;
    private  Joined<K, V1, V2> joined;


    private StreamStreamJoinNode( string nodeName,
                                  ValueJoiner<V1, V2, VR> valueJoiner,
                                  ProcessorParameters<K, V1> joinThisProcessorParameters,
                                  ProcessorParameters<K, V2> joinOtherProcessParameters,
                                  ProcessorParameters<K, VR> joinMergeProcessorParameters,
                                  ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters,
                                  ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters,
                                  StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder,
                                  StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder,
                                  Joined<K, V1, V2> joined)
{

        super(nodeName,
              valueJoiner,
              joinThisProcessorParameters,
              joinOtherProcessParameters,
              joinMergeProcessorParameters,
              null,
              null);

        this.thisWindowStoreBuilder = thisWindowStoreBuilder;
        this.otherWindowStoreBuilder = otherWindowStoreBuilder;
        this.joined = joined;
        this.thisWindowedStreamProcessorParameters = thisWindowedStreamProcessorParameters;
        this.otherWindowedStreamProcessorParameters =  otherWindowedStreamProcessorParameters;

    }


    
    public string ToString()
{
        return "StreamStreamJoinNode{" +
               "thisWindowedStreamProcessorParameters=" + thisWindowedStreamProcessorParameters +
               ", otherWindowedStreamProcessorParameters=" + otherWindowedStreamProcessorParameters +
               ", thisWindowStoreBuilder=" + thisWindowStoreBuilder +
               ", otherWindowStoreBuilder=" + otherWindowStoreBuilder +
               ", joined=" + joined +
               "} " + super.ToString();
    }

    
    public void writeToTopology( InternalTopologyBuilder topologyBuilder)
{

         string thisProcessorName = thisProcessorParameters().processorName();
         string otherProcessorName = otherProcessorParameters().processorName();
         string thisWindowedStreamProcessorName = thisWindowedStreamProcessorParameters.processorName();
         string otherWindowedStreamProcessorName = otherWindowedStreamProcessorParameters.processorName();

        topologyBuilder.AddProcessor(thisProcessorName, thisProcessorParameters().processorSupplier(), thisWindowedStreamProcessorName);
        topologyBuilder.AddProcessor(otherProcessorName, otherProcessorParameters().processorSupplier(), otherWindowedStreamProcessorName);
        topologyBuilder.AddProcessor(mergeProcessorParameters().processorName(), mergeProcessorParameters().processorSupplier(), thisProcessorName, otherProcessorName);
        topologyBuilder.AddStateStore(thisWindowStoreBuilder, thisWindowedStreamProcessorName, otherProcessorName);
        topologyBuilder.AddStateStore(otherWindowStoreBuilder, otherWindowedStreamProcessorName, thisProcessorName);
    }

    public static <K, V1, V2, VR> StreamStreamJoinNodeBuilder<K, V1, V2, VR> streamStreamJoinNodeBuilder()
{
        return new StreamStreamJoinNodeBuilder<>();
    }

    public static  class StreamStreamJoinNodeBuilder<K, V1, V2, VR> {

        private string nodeName;
        private ValueJoiner<V1, V2, VR> valueJoiner;
        private ProcessorParameters<K, V1> joinThisProcessorParameters;
        private ProcessorParameters<K, V2> joinOtherProcessorParameters;
        private ProcessorParameters<K, VR> joinMergeProcessorParameters;
        private ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters;
        private ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters;
        private StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder;
        private StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder;
        private Joined<K, V1, V2> joined;


        private StreamStreamJoinNodeBuilder()
{
        }


        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withValueJoiner( ValueJoiner<V1, V2, VR> valueJoiner)
{
            this.valueJoiner = valueJoiner;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinThisProcessorParameters( ProcessorParameters<K, V1> joinThisProcessorParameters)
{
            this.joinThisProcessorParameters = joinThisProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withNodeName( string nodeName)
{
            this.nodeName = nodeName;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinOtherProcessorParameters( ProcessorParameters<K, V2> joinOtherProcessParameters)
{
            this.joinOtherProcessorParameters = joinOtherProcessParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoinMergeProcessorParameters( ProcessorParameters<K, VR> joinMergeProcessorParameters)
{
            this.joinMergeProcessorParameters = joinMergeProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withThisWindowedStreamProcessorParameters( ProcessorParameters<K, V1> thisWindowedStreamProcessorParameters)
{
            this.thisWindowedStreamProcessorParameters = thisWindowedStreamProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOtherWindowedStreamProcessorParameters(
             ProcessorParameters<K, V2> otherWindowedStreamProcessorParameters)
{
            this.otherWindowedStreamProcessorParameters = otherWindowedStreamProcessorParameters;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withThisWindowStoreBuilder( StoreBuilder<WindowStore<K, V1>> thisWindowStoreBuilder)
{
            this.thisWindowStoreBuilder = thisWindowStoreBuilder;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withOtherWindowStoreBuilder( StoreBuilder<WindowStore<K, V2>> otherWindowStoreBuilder)
{
            this.otherWindowStoreBuilder = otherWindowStoreBuilder;
            return this;
        }

        public StreamStreamJoinNodeBuilder<K, V1, V2, VR> withJoined( Joined<K, V1, V2> joined)
{
            this.joined = joined;
            return this;
        }

        public StreamStreamJoinNode<K, V1, V2, VR> build()
{

            return new StreamStreamJoinNode<>(nodeName,
                                              valueJoiner,
                                              joinThisProcessorParameters,
                                              joinOtherProcessorParameters,
                                              joinMergeProcessorParameters,
                                              thisWindowedStreamProcessorParameters,
                                              otherWindowedStreamProcessorParameters,
                                              thisWindowStoreBuilder,
                                              otherWindowStoreBuilder,
                                              joined);


        }
    }
}
