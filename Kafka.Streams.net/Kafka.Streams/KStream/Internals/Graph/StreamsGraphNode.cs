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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;

public abstract class StreamsGraphNode {

    private  Collection<StreamsGraphNode> childNodes = new LinkedHashSet<>();
    private  Collection<StreamsGraphNode> parentNodes = new LinkedHashSet<>();
    private  string nodeName;
    private bool keyChangingOperation;
    private bool valueChangingOperation;
    private bool mergeNode;
    private Integer buildPriority;
    private bool hasWrittenToTopology = false;

    public StreamsGraphNode( string nodeName) {
        this.nodeName = nodeName;
    }

    public Collection<StreamsGraphNode> parentNodes() {
        return parentNodes;
    }

    string[] parentNodeNames() {
         string[] parentNames = new string[parentNodes.size()];
        int index = 0;
        for ( StreamsGraphNode parentNode : parentNodes) {
            parentNames[index++] = parentNode.nodeName();
        }
        return parentNames;
    }

    public bool allParentsWrittenToTopology() {
        for ( StreamsGraphNode parentNode : parentNodes) {
            if (!parentNode.hasWrittenToTopology()) {
                return false;
            }
        }
        return true;
    }

    public Collection<StreamsGraphNode> children() {
        return new LinkedHashSet<>(childNodes);
    }

    public void clearChildren() {
        for ( StreamsGraphNode childNode : childNodes) {
            childNode.parentNodes.remove(this);
        }
        childNodes.clear();
    }

    public bool removeChild( StreamsGraphNode child) {
        return childNodes.remove(child) && child.parentNodes.remove(this);
    }

    public void addChild( StreamsGraphNode childNode) {
        this.childNodes.add(childNode);
        childNode.parentNodes.add(this);
    }

    public string nodeName() {
        return nodeName;
    }

    public bool isKeyChangingOperation() {
        return keyChangingOperation;
    }

    public bool isValueChangingOperation() {
        return valueChangingOperation;
    }

    public bool isMergeNode() {
        return mergeNode;
    }

    public void setMergeNode( bool mergeNode) {
        this.mergeNode = mergeNode;
    }

    public void setValueChangingOperation( bool valueChangingOperation) {
        this.valueChangingOperation = valueChangingOperation;
    }

    public void keyChangingOperation( bool keyChangingOperation) {
        this.keyChangingOperation = keyChangingOperation;
    }

    public void setBuildPriority( int buildPriority) {
        this.buildPriority = buildPriority;
    }

    public Integer buildPriority() {
        return this.buildPriority;
    }

    public abstract void writeToTopology( InternalTopologyBuilder topologyBuilder);

    public bool hasWrittenToTopology() {
        return hasWrittenToTopology;
    }

    public void setHasWrittenToTopology( bool hasWrittenToTopology) {
        this.hasWrittenToTopology = hasWrittenToTopology;
    }

    @Override
    public string toString() {
         string[] parentNames = parentNodeNames();
        return "StreamsGraphNode{" +
               "nodeName='" + nodeName + '\'' +
               ", buildPriority=" + buildPriority +
               ", hasWrittenToTopology=" + hasWrittenToTopology +
               ", keyChangingOperation=" + keyChangingOperation +
               ", valueChangingOperation=" + valueChangingOperation +
               ", mergeNode=" + mergeNode +
               ", parentNodes=" + Arrays.toString(parentNames) + '}';
    }
}
