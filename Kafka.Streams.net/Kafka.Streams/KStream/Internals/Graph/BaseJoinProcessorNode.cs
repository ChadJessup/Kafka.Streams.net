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

namespace Kafka.Streams.KStream.Internals.Graph {



/**
 * Utility base containing the common fields between
 * a Stream-Stream join and a Table-Table join
 */
abstract BaseJoinProcessorNode<K, V1, V2, VR> : StreamsGraphNode {

    private  ProcessorParameters<K, V1> joinThisProcessorParameters;
    private  ProcessorParameters<K, V2> joinOtherProcessorParameters;
    private  ProcessorParameters<K, VR> joinMergeProcessorParameters;
    private  ValueJoiner<V1, V2, VR> valueJoiner;
    private  string thisJoinSideNodeName;
    private  string otherJoinSideNodeName;


    BaseJoinProcessorNode( string nodeName,
                           ValueJoiner<V1, V2, VR> valueJoiner,
                           ProcessorParameters<K, V1> joinThisProcessorParameters,
                           ProcessorParameters<K, V2> joinOtherProcessorParameters,
                           ProcessorParameters<K, VR> joinMergeProcessorParameters,
                           string thisJoinSideNodeName,
                           string otherJoinSideNodeName)
{

        base(nodeName);

        this.valueJoiner = valueJoiner;
        this.joinThisProcessorParameters = joinThisProcessorParameters;
        this.joinOtherProcessorParameters = joinOtherProcessorParameters;
        this.joinMergeProcessorParameters = joinMergeProcessorParameters;
        this.thisJoinSideNodeName = thisJoinSideNodeName;
        this.otherJoinSideNodeName = otherJoinSideNodeName;
    }

    ProcessorParameters<K, V1> thisProcessorParameters()
{
        return joinThisProcessorParameters;
    }

    ProcessorParameters<K, V2> otherProcessorParameters()
{
        return joinOtherProcessorParameters;
    }

    ProcessorParameters<K, VR> mergeProcessorParameters()
{
        return joinMergeProcessorParameters;
    }

    ValueJoiner<V1, V2, VR> valueJoiner()
{
        return valueJoiner;
    }

    string thisJoinSideNodeName()
{
        return thisJoinSideNodeName;
    }

    string otherJoinSideNodeName()
{
        return otherJoinSideNodeName;
    }

    
    public string ToString()
{
        return "BaseJoinProcessorNode{" +
               "joinThisProcessorParameters=" + joinThisProcessorParameters +
               ", joinOtherProcessorParameters=" + joinOtherProcessorParameters +
               ", joinMergeProcessorParameters=" + joinMergeProcessorParameters +
               ", valueJoiner=" + valueJoiner +
               ", thisJoinSideNodeName='" + thisJoinSideNodeName + '\'' +
               ", otherJoinSideNodeName='" + otherJoinSideNodeName + '\'' +
               "} " + base.ToString();
    }
}
