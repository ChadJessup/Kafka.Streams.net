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
package org.apache.kafka.streams.state.internals.metrics;

using Kafka.Common.MetricName;
using Kafka.Common.metrics.Sensor;
using Kafka.Common.metrics.stats.Avg;
using Kafka.Common.metrics.stats.Max;
using Kafka.Common.metrics.stats.Value;
using Kafka.Streams.Processor.IStateStore;
using Kafka.Streams.Processor.internals.InternalProcessorContext;
using Kafka.Streams.Processor.internals.metrics.StreamsMetricsImpl;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgMaxLatency;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;

public class Sensors {
    private Sensors() {}

    public static Sensor createTaskAndStoreLatencyAndThroughputSensors(Sensor.RecordingLevel level,
                                                                       string operation,
                                                                       StreamsMetricsImpl metrics,
                                                                       string metricsGroup,
                                                                       string taskName,
                                                                       string storeName,
                                                                       Dictionary<string, string> taskTags,
                                                                       Dictionary<string, string> storeTags) {
        Sensor taskSensor = metrics.taskLevelSensor(taskName, operation, level);
        addAvgMaxLatency(taskSensor, metricsGroup, taskTags, operation);
        addInvocationRateAndCount(taskSensor, metricsGroup, taskTags, operation);
        Sensor sensor = metrics.storeLevelSensor(taskName, storeName, operation, level, taskSensor);
        addAvgMaxLatency(sensor, metricsGroup, storeTags, operation);
        addInvocationRateAndCount(sensor, metricsGroup, storeTags, operation);
        return sensor;
    }

    public static Sensor createBufferSizeSensor(IStateStore store,
                                                InternalProcessorContext context) {
        return getBufferSizeOrCountSensor(store, context, "size");
    }

    public static Sensor createBufferCountSensor(IStateStore store,
                                                 InternalProcessorContext context) {
        return getBufferSizeOrCountSensor(store, context, "count");
    }

    private static Sensor getBufferSizeOrCountSensor(IStateStore store,
                                                     InternalProcessorContext context,
                                                     string property) {
        StreamsMetricsImpl metrics = context.metrics();

        string sensorName = "suppression-buffer-" + property;

        Sensor sensor = metrics.storeLevelSensor(
            context.taskId().toString(),
            store.name(),
            sensorName,
            Sensor.RecordingLevel.DEBUG
        );

        string metricsGroup = "stream-buffer-metrics";

        Dictionary<string, string> tags = metrics.tagMap(
            "task-id", context.taskId().toString(),
            "buffer-id", store.name()
        );

        sensor.add(
            new MetricName(
                sensorName + "-current",
                metricsGroup,
                "The current " + property + " of buffered records.",
                tags),
            new Value()
        );


        sensor.add(
            new MetricName(
                sensorName + "-avg",
                metricsGroup,
                "The average " + property + " of buffered records.",
                tags),
            new Avg()
        );

        sensor.add(
            new MetricName(
                sensorName + "-max",
                metricsGroup,
                "The max " + property + " of buffered records.",
                tags),
            new Max()
        );

        return sensor;
    }
}

