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
using Kafka.Common;
using Kafka.Common.Metrics;
using Kafka.Common.Metrics.Stats;
using Kafka.Streams.Processor.Internals.metrics;
using Kafka.Streams.Processor.Internals.Metrics;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class TaskMetrics
    {
        StreamsMetricsImpl metrics;
        Sensor taskCommitTimeSensor;
        Sensor taskEnforcedProcessSensor;
        private string taskName;

        TaskMetrics(TaskId id, StreamsMetricsImpl metrics)
        {
            taskName = id.ToString();
            this.metrics = metrics;
            string group = "stream-task-metrics";

            // first.Add the global operation metrics if not yet, with the global tags only
            Sensor parent = ThreadMetrics.commitOverTasksSensor(metrics);

            //.Add the operation metrics with.Additional tags
            Dictionary<string, string> tagMap = metrics.tagMap("task-id", taskName);
            taskCommitTimeSensor = metrics.taskLevelSensor(taskName, "commit", RecordingLevel.DEBUG, parent);
            taskCommitTimeSensor.Add(
                new MetricName("commit-latency-avg", group, "The average latency of commit operation.", tagMap),
                new Avg()
            );
            taskCommitTimeSensor.Add(
                new MetricName("commit-latency-max", group, "The max latency of commit operation.", tagMap),
                new Max()
            );
            taskCommitTimeSensor.Add(
                new MetricName("commit-rate", group, "The average number of occurrence of commit operation per second.", tagMap),
                new Rate(TimeUnit.SECONDS, new WindowedCount())
            );
            taskCommitTimeSensor.Add(
                new MetricName("commit-total", group, "The total number of occurrence of commit operations.", tagMap),
                new CumulativeCount()
            );

            //.Add the metrics for enforced processing
            taskEnforcedProcessSensor = metrics.taskLevelSensor(taskName, "enforced-processing", RecordingLevel.DEBUG, parent);
            taskEnforcedProcessSensor.Add(
                    new MetricName("enforced-processing-rate", group, "The average number of occurrence of enforced-processing operation per second.", tagMap),
                    new Rate(TimeUnit.SECONDS, new WindowedCount())
            );
            taskEnforcedProcessSensor.Add(
                    new MetricName("enforced-processing-total", group, "The total number of occurrence of enforced-processing operations.", tagMap),
                    new CumulativeCount()
            );

        }

        void removeAllSensors()
        {
            metrics.removeAllTaskLevelSensors(taskName);
        }
    }
}