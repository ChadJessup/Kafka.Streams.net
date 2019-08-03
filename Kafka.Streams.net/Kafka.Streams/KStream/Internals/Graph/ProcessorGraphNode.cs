/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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

package org.apache.kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

/**
 * Used to represent any type of stateless operation:
 *
 * map, mapValues, flatMap, flatMapValues, filter, filterNot, branch
 */
public class ProcessorGraphNode<K, V> : StreamsGraphNode {

    private  ProcessorParameters<K, V> processorParameters;

    public ProcessorGraphNode( string nodeName,
                               ProcessorParameters<K, V> processorParameters) {

        super(nodeName);

        this.processorParameters = processorParameters;
    }

    public ProcessorParameters processorParameters() {
        return processorParameters;
    }

    @Override
    public string toString() {
        return "ProcessorNode{" +
               "processorParameters=" + processorParameters +
               "} " + super.toString();
    }

    @Override
    public void writeToTopology( InternalTopologyBuilder topologyBuilder) {

        topologyBuilder.addProcessor(processorParameters.processorName(), processorParameters.processorSupplier(), parentNodeNames());
    }
}
