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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.IStateStore;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcessorTopology {

    private List<ProcessorNode> processorNodes;
    private Dictionary<string, SourceNode> sourcesByTopic;
    private Dictionary<string, SinkNode> sinksByTopic;
    private List<IStateStore> stateStores;
    private List<IStateStore> globalStateStores;
    private Dictionary<string, string> storeToChangelogTopic;
    private Set<string> repartitionTopics;

    public ProcessorTopology(List<ProcessorNode> processorNodes,
                             Dictionary<string, SourceNode> sourcesByTopic,
                             Dictionary<string, SinkNode> sinksByTopic,
                             List<IStateStore> stateStores,
                             List<IStateStore> globalStateStores,
                             Dictionary<string, string> storeToChangelogTopic,
                             Set<string> repartitionTopics) {
        this.processorNodes = Collections.unmodifiableList(processorNodes);
        this.sourcesByTopic = Collections.unmodifiableMap(sourcesByTopic);
        this.sinksByTopic = Collections.unmodifiableMap(sinksByTopic);
        this.stateStores = Collections.unmodifiableList(stateStores);
        this.globalStateStores = Collections.unmodifiableList(globalStateStores);
        this.storeToChangelogTopic = Collections.unmodifiableMap(storeToChangelogTopic);
        this.repartitionTopics = Collections.unmodifiableSet(repartitionTopics);
    }

    public Set<string> sourceTopics() {
        return sourcesByTopic.keySet();
    }

    public SourceNode source(string topic) {
        return sourcesByTopic.get(topic);
    }

    public Set<SourceNode> sources() {
        return new HashSet<>(sourcesByTopic.values());
    }

    public Set<string> sinkTopics() {
        return sinksByTopic.keySet();
    }

    public SinkNode sink(string topic) {
        return sinksByTopic.get(topic);
    }

    public Set<SinkNode> sinks() {
        return new HashSet<>(sinksByTopic.values());
    }

    public List<ProcessorNode> processors() {
        return processorNodes;
    }

    public List<IStateStore> stateStores() {
        return stateStores;
    }

    public List<IStateStore> globalStateStores() {
        return globalStateStores;
    }

    public Dictionary<string, string> storeToChangelogTopic() {
        return storeToChangelogTopic;
    }

    bool isRepartitionTopic(string topic) {
        return repartitionTopics.contains(topic);
    }

    public bool hasPersistentLocalStore() {
        for (IStateStore store : stateStores) {
            if (store.persistent()) {
                return true;
            }
        }
        return false;
    }

    public bool hasPersistentGlobalStore() {
        for (IStateStore store : globalStateStores) {
            if (store.persistent()) {
                return true;
            }
        }
        return false;
    }

    private string childrenToString(string indent, List<ProcessorNode<?, ?>> children) {
        if (children == null || children.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder(indent + "\tchildren:\t[");
        for (ProcessorNode child : children) {
            sb.append(child.name());
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);  // remove the last comma
        sb.append("]\n");

        // recursively print children
        for (ProcessorNode<?, ?> child : children) {
            sb.append(child.toString(indent)).append(childrenToString(indent, child.children()));
        }
        return sb.toString();
    }

    /**
     * Produces a string representation containing useful information this topology starting with the given indent.
     * This is useful in debugging scenarios.
     * @return A string representation of this instance.
     */
    @Override
    public string toString() {
        return toString("");
    }

    /**
     * Produces a string representation containing useful information this topology.
     * This is useful in debugging scenarios.
     * @return A string representation of this instance.
     */
    public string toString(string indent) {
        StringBuilder sb = new StringBuilder(indent + "ProcessorTopology:\n");

        // start from sources
        for (SourceNode<?, ?> source : sourcesByTopic.values()) {
            sb.append(source.toString(indent + "\t")).append(childrenToString(indent + "\t", source.children()));
        }
        return sb.toString();
    }

    // for testing only
    public Set<string> processorConnectedStateStores(string processorName) {
        for (ProcessorNode<?, ?> node : processorNodes) {
            if (node.name().Equals(processorName)) {
                return node.stateStores;
            }
        }

        return Collections.emptySet();
    }
}
