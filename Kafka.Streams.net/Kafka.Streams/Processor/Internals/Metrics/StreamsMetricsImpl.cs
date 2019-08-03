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
package org.apache.kafka.streams.processor.internals.metrics;

using Kafka.Common.Metric;
using Kafka.Common.MetricName;
using Kafka.Common.metrics.Metrics;
using Kafka.Common.metrics.Sensor;
using Kafka.Common.metrics.Sensor.RecordingLevel;
using Kafka.Common.metrics.stats.Avg;
using Kafka.Common.metrics.stats.CumulativeCount;
using Kafka.Common.metrics.stats.Max;
using Kafka.Common.metrics.stats.Rate;
using Kafka.Common.metrics.stats.WindowedCount;
import org.apache.kafka.streams.StreamsMetrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class StreamsMetricsImpl implements StreamsMetrics {
    private Metrics metrics;
    private Dictionary<Sensor, Sensor> parentSensors;
    private string threadName;

    private Deque<string> threadLevelSensors = new LinkedList<>();
    private Dictionary<string, Deque<string>> taskLevelSensors = new HashMap<>();
    private Dictionary<string, Deque<string>> nodeLevelSensors = new HashMap<>();
    private Dictionary<string, Deque<string>> cacheLevelSensors = new HashMap<>();
    private Dictionary<string, Deque<string>> storeLevelSensors = new HashMap<>();

    private static string SENSOR_PREFIX_DELIMITER = ".";
    private static string SENSOR_NAME_DELIMITER = ".s.";

    public static string THREAD_ID_TAG = "client-id";
    public static string TASK_ID_TAG = "task-id";

    public static string ALL_TASKS = "all";

    public static string LATENCY_SUFFIX = "-latency";
    public static string AVG_SUFFIX = "-avg";
    public static string MAX_SUFFIX = "-max";
    public static string RATE_SUFFIX = "-rate";
    public static string TOTAL_SUFFIX = "-total";

    public static string THREAD_LEVEL_GROUP = "stream-metrics";
    public static string TASK_LEVEL_GROUP = "stream-task-metrics";

    public static string PROCESSOR_NODE_METRICS_GROUP = "stream-processor-node-metrics";
    public static string PROCESSOR_NODE_ID_TAG = "processor-node-id";

    public static string EXPIRED_WINDOW_RECORD_DROP = "expired-window-record-drop";
    public static string LATE_RECORD_DROP = "late-record-drop";

    public StreamsMetricsImpl(Metrics metrics, string threadName) {
        Objects.requireNonNull(metrics, "Metrics cannot be null");
        this.metrics = metrics;
        this.threadName = threadName;

        this.parentSensors = new HashMap<>();
    }

    public Sensor threadLevelSensor(string sensorName,
                                          RecordingLevel recordingLevel,
                                          Sensor... parents) {
        synchronized (threadLevelSensors) {
            string fullSensorName = threadSensorPrefix() + SENSOR_NAME_DELIMITER + sensorName;
            Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);
            threadLevelSensors.push(fullSensorName);
            return sensor;
        }
    }

    private string threadSensorPrefix() {
        return "internal" + SENSOR_PREFIX_DELIMITER + threadName;
    }

    public Dictionary<string, string> threadLevelTagMap() {
        Dictionary<string, string> tagMap = new LinkedHashMap<>();
        tagMap.put(THREAD_ID_TAG, threadName);
        return tagMap;
    }

    public Dictionary<string, string> threadLevelTagMap(string... tags) {
        Dictionary<string, string> tagMap = threadLevelTagMap();
        if (tags != null) {
            if ((tags.length % 2) != 0) {
                throw new IllegalArgumentException("Tags needs to be specified in key-value pairs");
            }

            for (int i = 0; i < tags.length; i += 2) {
                tagMap.put(tags[i], tags[i + 1]);
            }
        }
        return tagMap;
    }

    public void removeAllThreadLevelSensors() {
        synchronized (threadLevelSensors) {
            while (!threadLevelSensors.isEmpty()) {
                metrics.removeSensor(threadLevelSensors.pop());
            }
        }
    }

    public Sensor taskLevelSensor(string taskName,
                                        string sensorName,
                                        RecordingLevel recordingLevel,
                                        Sensor... parents) {
        string key = taskSensorPrefix(taskName);
        synchronized (taskLevelSensors) {
            if (!taskLevelSensors.containsKey(key)) {
                taskLevelSensors.put(key, new LinkedList<>());
            }

            string fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;

            Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

            taskLevelSensors.get(key).push(fullSensorName);

            return sensor;
        }
    }

    public void removeAllTaskLevelSensors(string taskName) {
        string key = taskSensorPrefix(taskName);
        synchronized (taskLevelSensors) {
            Deque<string> sensors = taskLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
    }

    private string taskSensorPrefix(string taskName) {
        return threadSensorPrefix() + SENSOR_PREFIX_DELIMITER + "task" + SENSOR_PREFIX_DELIMITER + taskName;
    }

    public Sensor nodeLevelSensor(string taskName,
                                  string processorNodeName,
                                  string sensorName,
                                  Sensor.RecordingLevel recordingLevel,
                                  Sensor... parents) {
        string key = nodeSensorPrefix(taskName, processorNodeName);
        synchronized (nodeLevelSensors) {
            if (!nodeLevelSensors.containsKey(key)) {
                nodeLevelSensors.put(key, new LinkedList<>());
            }

            string fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;

            Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

            nodeLevelSensors.get(key).push(fullSensorName);

            return sensor;
        }
    }

    public void removeAllNodeLevelSensors(string taskName, string processorNodeName) {
        string key = nodeSensorPrefix(taskName, processorNodeName);
        synchronized (nodeLevelSensors) {
            Deque<string> sensors = nodeLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
    }

    private string nodeSensorPrefix(string taskName, string processorNodeName) {
        return taskSensorPrefix(taskName) + SENSOR_PREFIX_DELIMITER + "node" + SENSOR_PREFIX_DELIMITER + processorNodeName;
    }

    public Sensor cacheLevelSensor(string taskName,
                                         string cacheName,
                                         string sensorName,
                                         Sensor.RecordingLevel recordingLevel,
                                         Sensor... parents) {
        string key = cacheSensorPrefix(taskName, cacheName);
        synchronized (cacheLevelSensors) {
            if (!cacheLevelSensors.containsKey(key)) {
                cacheLevelSensors.put(key, new LinkedList<>());
            }

            string fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;

            Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

            cacheLevelSensors.get(key).push(fullSensorName);

            return sensor;
        }
    }

    public void removeAllCacheLevelSensors(string taskName, string cacheName) {
        string key = cacheSensorPrefix(taskName, cacheName);
        synchronized (cacheLevelSensors) {
            Deque<string> strings = cacheLevelSensors.remove(key);
            while (strings != null && !strings.isEmpty()) {
                metrics.removeSensor(strings.pop());
            }
        }
    }

    private string cacheSensorPrefix(string taskName, string cacheName) {
        return taskSensorPrefix(taskName) + SENSOR_PREFIX_DELIMITER + "cache" + SENSOR_PREFIX_DELIMITER + cacheName;
    }

    public Sensor storeLevelSensor(string taskName,
                                         string storeName,
                                         string sensorName,
                                         Sensor.RecordingLevel recordingLevel,
                                         Sensor... parents) {
        string key = storeSensorPrefix(taskName, storeName);
        synchronized (storeLevelSensors) {
            if (!storeLevelSensors.containsKey(key)) {
                storeLevelSensors.put(key, new LinkedList<>());
            }

            string fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;

            Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

            storeLevelSensors.get(key).push(fullSensorName);

            return sensor;
        }
    }

    public void removeAllStoreLevelSensors(string taskName, string storeName) {
        string key = storeSensorPrefix(taskName, storeName);
        synchronized (storeLevelSensors) {
            Deque<string> sensors = storeLevelSensors.remove(key);
            while (sensors != null && !sensors.isEmpty()) {
                metrics.removeSensor(sensors.pop());
            }
        }
    }

    private string storeSensorPrefix(string taskName, string storeName) {
        return taskSensorPrefix(taskName) + SENSOR_PREFIX_DELIMITER + "store" + SENSOR_PREFIX_DELIMITER + storeName;
    }

    @Override
    public Sensor addSensor(string name, Sensor.RecordingLevel recordingLevel) {
        return metrics.sensor(name, recordingLevel);
    }

    @Override
    public Sensor addSensor(string name, Sensor.RecordingLevel recordingLevel, Sensor... parents) {
        return metrics.sensor(name, recordingLevel, parents);
    }

    @Override
    public Dictionary<MetricName, ? : Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    @Override
    public void recordLatency(Sensor sensor, long startNs, long endNs) {
        sensor.record(endNs - startNs);
    }

    @Override
    public void recordThroughput(Sensor sensor, long value) {
        sensor.record(value);
    }

    public Dictionary<string, string> tagMap(string... tags) {
        Dictionary<string, string> tagMap = new LinkedHashMap<>();
        tagMap.put("client-id", threadName);
        if (tags != null) {
            if ((tags.length % 2) != 0) {
                throw new IllegalArgumentException("Tags needs to be specified in key-value pairs");
            }

            for (int i = 0; i < tags.length; i += 2) {
                tagMap.put(tags[i], tags[i + 1]);
            }
        }
        return tagMap;
    }


    private Dictionary<string, string> constructTags(string scopeName, string entityName, string... tags) {
        string[] updatedTags = Arrays.copyOf(tags, tags.length + 2);
        updatedTags[tags.length] = scopeName + "-id";
        updatedTags[tags.length + 1] = entityName;
        return tagMap(updatedTags);
    }


    /**
     * @throws IllegalArgumentException if tags is not constructed in key-value pairs
     */
    @Override
    public Sensor addLatencyAndThroughputSensor(string scopeName,
                                                string entityName,
                                                string operationName,
                                                Sensor.RecordingLevel recordingLevel,
                                                string... tags) {
        string group = groupNameFromScope(scopeName);

        Dictionary<string, string> tagMap = constructTags(scopeName, entityName, tags);
        Dictionary<string, string> allTagMap = constructTags(scopeName, "all", tags);

        // first add the global operation metrics if not yet, with the global tags only
        Sensor parent = metrics.sensor(externalParentSensorName(operationName), recordingLevel);
        addAvgMaxLatency(parent, group, allTagMap, operationName);
        addInvocationRateAndCount(parent, group, allTagMap, operationName);

        // add the operation metrics with additional tags
        Sensor sensor = metrics.sensor(externalChildSensorName(operationName, entityName), recordingLevel, parent);
        addAvgMaxLatency(sensor, group, tagMap, operationName);
        addInvocationRateAndCount(sensor, group, tagMap, operationName);

        parentSensors.put(sensor, parent);

        return sensor;

    }

    /**
     * @throws IllegalArgumentException if tags is not constructed in key-value pairs
     */
    @Override
    public Sensor addThroughputSensor(string scopeName,
                                      string entityName,
                                      string operationName,
                                      Sensor.RecordingLevel recordingLevel,
                                      string... tags) {
        string group = groupNameFromScope(scopeName);

        Dictionary<string, string> tagMap = constructTags(scopeName, entityName, tags);
        Dictionary<string, string> allTagMap = constructTags(scopeName, "all", tags);

        // first add the global operation metrics if not yet, with the global tags only
        Sensor parent = metrics.sensor(externalParentSensorName(operationName), recordingLevel);
        addInvocationRateAndCount(parent, group, allTagMap, operationName);

        // add the operation metrics with additional tags
        Sensor sensor = metrics.sensor(externalChildSensorName(operationName, entityName), recordingLevel, parent);
        addInvocationRateAndCount(sensor, group, tagMap, operationName);

        parentSensors.put(sensor, parent);

        return sensor;

    }

    private string externalChildSensorName(string operationName, string entityName) {
        return "external" + SENSOR_PREFIX_DELIMITER + threadName
            + SENSOR_PREFIX_DELIMITER + "entity" + SENSOR_PREFIX_DELIMITER + entityName
            + SENSOR_NAME_DELIMITER + operationName;
    }

    private string externalParentSensorName(string operationName) {
        return "external" + SENSOR_PREFIX_DELIMITER + threadName + SENSOR_NAME_DELIMITER + operationName;
    }


    public static void addAvgAndMax(Sensor sensor,
                                    string group,
                                    Dictionary<string, string> tags,
                                    string operation) {
        sensor.add(
            new MetricName(
                operation + AVG_SUFFIX,
                group,
                "The average value of " + operation + ".",
                tags),
            new Avg()
        );
        sensor.add(
            new MetricName(
                operation + MAX_SUFFIX,
                group,
                "The max value of " + operation + ".",
                tags),
            new Max()
        );
    }

    public static void addAvgMaxLatency(Sensor sensor,
                                        string group,
                                        Dictionary<string, string> tags,
                                        string operation) {
        sensor.add(
            new MetricName(
                operation + "-latency-avg",
                group,
                "The average latency of " + operation + " operation.",
                tags),
            new Avg()
        );
        sensor.add(
            new MetricName(
                operation + "-latency-max",
                group,
                "The max latency of " + operation + " operation.",
                tags),
            new Max()
        );
    }

    public static void addInvocationRateAndCount(Sensor sensor,
                                                 string group,
                                                 Dictionary<string, string> tags,
                                                 string operation,
                                                 string descriptionOfInvocation,
                                                 string descriptionOfRate) {
        sensor.add(
            new MetricName(
                operation + TOTAL_SUFFIX,
                group,
                descriptionOfInvocation,
                tags
            ),
            new CumulativeCount()
        );
        sensor.add(
            new MetricName(
                operation + RATE_SUFFIX,
                group,
                descriptionOfRate,
                tags
            ),
            new Rate(TimeUnit.SECONDS, new WindowedCount())
        );
    }

    public static void addInvocationRateAndCount(Sensor sensor,
                                                 string group,
                                                 Dictionary<string, string> tags,
                                                 string operation) {
        addInvocationRateAndCount(sensor,
                                  group,
                                  tags,
                                  operation,
                                  "The total number of " + operation,
                                  "The average per-second number of " + operation);
    }

    /**
     * Deletes a sensor and its parents, if any
     */
    @Override
    public void removeSensor(Sensor sensor) {
        Objects.requireNonNull(sensor, "Sensor is null");
        metrics.removeSensor(sensor.name());

        Sensor parent = parentSensors.remove(sensor);
        if (parent != null) {
            metrics.removeSensor(parent.name());
        }
    }

    /**
     * Visible for testing
     */
    Dictionary<Sensor, Sensor> parentSensors() {
        return Collections.unmodifiableMap(parentSensors);
    }

    private static string groupNameFromScope(string scopeName) {
        return "stream-" + scopeName + "-metrics";
    }
}
