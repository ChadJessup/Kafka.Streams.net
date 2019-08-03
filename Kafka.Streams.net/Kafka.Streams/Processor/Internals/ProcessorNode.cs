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

using Kafka.Common.metrics.Sensor;
using Kafka.Common.Utils.SystemTime;
using Kafka.Common.Utils.Time;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.IProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_METRICS_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgMaxLatency;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;

public class ProcessorNode<K, V> {

    // TODO: 'children' can be removed when #forward() via index is removed
    private List<ProcessorNode<?, ?>> children;
    private Dictionary<string, ProcessorNode<?, ?>> childByName;

    private NodeMetrics nodeMetrics;
    private Processor<K, V> processor;
    private string name;
    private Time time;

    public Set<string> stateStores;

    public ProcessorNode(string name) {
        this(name, null, null);
    }


    public ProcessorNode(string name, Processor<K, V> processor, Set<string> stateStores) {
        this.name = name;
        this.processor = processor;
        this.children = new ArrayList<>();
        this.childByName = new HashMap<>();
        this.stateStores = stateStores;
        this.time = new SystemTime();
    }


    public string name() {
        return name;
    }

    public Processor<K, V> processor() {
        return processor;
    }

    public List<ProcessorNode<?, ?>> children() {
        return children;
    }

    ProcessorNode getChild(string childName) {
        return childByName.get(childName);
    }

    public void addChild(ProcessorNode<?, ?> child) {
        children.add(child);
        childByName.put(child.name, child);
    }

    public void init(InternalProcessorContext context) {
        try {
            nodeMetrics = new NodeMetrics(context.metrics(), name, context);
            long startNs = time.nanoseconds();
            if (processor != null) {
                processor.init(context);
            }
            nodeMetrics.nodeCreationSensor.record(time.nanoseconds() - startNs);
        } catch (Exception e) {
            throw new StreamsException(string.format("failed to initialize processor %s", name), e);
        }
    }

    public void close() {
        try {
            long startNs = time.nanoseconds();
            if (processor != null) {
                processor.close();
            }
            nodeMetrics.nodeDestructionSensor.record(time.nanoseconds() - startNs);
            nodeMetrics.removeAllSensors();
        } catch (Exception e) {
            throw new StreamsException(string.format("failed to close processor %s", name), e);
        }
    }


    public void process(K key, V value) {
        long startNs = time.nanoseconds();
        processor.process(key, value);
        nodeMetrics.nodeProcessTimeSensor.record(time.nanoseconds() - startNs);
    }

    public void punctuate(long timestamp, Punctuator punctuator) {
        long startNs = time.nanoseconds();
        punctuator.punctuate(timestamp);
        nodeMetrics.nodePunctuateTimeSensor.record(time.nanoseconds() - startNs);
    }

    /**
     * @return a string representation of this node, useful for debugging.
     */
    @Override
    public string toString() {
        return toString("");
    }

    /**
     * @return a string representation of this node starting with the given indent, useful for debugging.
     */
    public string toString(string indent) {
        StringBuilder sb = new StringBuilder(indent + name + ":\n");
        if (stateStores != null && !stateStores.isEmpty()) {
            sb.append(indent).append("\tstates:\t\t[");
            for (string store : stateStores) {
                sb.append(store);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);  // remove the last comma
            sb.append("]\n");
        }
        return sb.toString();
    }

    Sensor sourceNodeForwardSensor() {
        return nodeMetrics.sourceNodeForwardSensor;
    }

    private static class NodeMetrics {
        private StreamsMetricsImpl metrics;

        private Sensor nodeProcessTimeSensor;
        private Sensor nodePunctuateTimeSensor;
        private Sensor sourceNodeForwardSensor;
        private Sensor nodeCreationSensor;
        private Sensor nodeDestructionSensor;
        private string taskName;
        private string processorNodeName;

        private NodeMetrics(StreamsMetricsImpl metrics, string processorNodeName, IProcessorContext context) {
            this.metrics = metrics;

            string taskName = context.taskId().toString();
            Dictionary<string, string> tagMap = metrics.tagMap("task-id", context.taskId().toString(), PROCESSOR_NODE_ID_TAG, processorNodeName);
            Dictionary<string, string> allTagMap = metrics.tagMap("task-id", context.taskId().toString(), PROCESSOR_NODE_ID_TAG, "all");

            nodeProcessTimeSensor = createTaskAndNodeLatencyAndThroughputSensors(
                "process",
                metrics,
                taskName,
                processorNodeName,
                allTagMap,
                tagMap
            );

            nodePunctuateTimeSensor = createTaskAndNodeLatencyAndThroughputSensors(
                "punctuate",
                metrics,
                taskName,
                processorNodeName,
                allTagMap,
                tagMap
            );

            nodeCreationSensor = createTaskAndNodeLatencyAndThroughputSensors(
                "create",
                metrics,
                taskName,
                processorNodeName,
                allTagMap,
                tagMap
            );

            // note: this metric can be removed in the future, as it is only recorded before being immediately removed
            nodeDestructionSensor = createTaskAndNodeLatencyAndThroughputSensors(
                "destroy",
                metrics,
                taskName,
                processorNodeName,
                allTagMap,
                tagMap
            );

            sourceNodeForwardSensor = createTaskAndNodeLatencyAndThroughputSensors(
                "forward",
                metrics,
                taskName,
                processorNodeName,
                allTagMap,
                tagMap
            );

            this.taskName = taskName;
            this.processorNodeName = processorNodeName;
        }

        private void removeAllSensors() {
            metrics.removeAllNodeLevelSensors(taskName, processorNodeName);
        }

        private static Sensor createTaskAndNodeLatencyAndThroughputSensors(string operation,
                                                                           StreamsMetricsImpl metrics,
                                                                           string taskName,
                                                                           string processorNodeName,
                                                                           Dictionary<string, string> taskTags,
                                                                           Dictionary<string, string> nodeTags) {
            Sensor parent = metrics.taskLevelSensor(taskName, operation, Sensor.RecordingLevel.DEBUG);
            addAvgMaxLatency(parent, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);
            addInvocationRateAndCount(parent, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);

            Sensor sensor = metrics.nodeLevelSensor(taskName, processorNodeName, operation, Sensor.RecordingLevel.DEBUG, parent);
            addAvgMaxLatency(sensor, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);
            addInvocationRateAndCount(sensor, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);

            return sensor;
        }
    }
}
