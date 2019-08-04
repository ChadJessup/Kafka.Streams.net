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
namespace Kafka.streams.processor.internals.metrics;

using Kafka.Common.metrics.Sensor;
using Kafka.Common.metrics.RecordingLevel;

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ALL_TASKS;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_ID_TAG;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TASK_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.THREAD_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMax;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;

public class ThreadMetrics {
    private ThreadMetrics() {}

    private static string COMMIT = "commit";
    private static string POLL = "poll";
    private static string PROCESS = "process";
    private static string PUNCTUATE = "punctuate";
    private static string CREATE_TASK = "task-created";
    private static string CLOSE_TASK = "task-closed";
    private static string SKIP_RECORD = "skipped-records";

    private static string TOTAL_DESCRIPTION = "The total number of ";
    private static string RATE_DESCRIPTION = "The average per-second number of ";
    private static string COMMIT_DESCRIPTION = "commit calls";
    private static string COMMIT_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + COMMIT_DESCRIPTION;
    private static string COMMIT_RATE_DESCRIPTION = RATE_DESCRIPTION + COMMIT_DESCRIPTION;
    private static string CREATE_TASK_DESCRIPTION = "newly created tasks";
    private static string CREATE_TASK_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + CREATE_TASK_DESCRIPTION;
    private static string CREATE_TASK_RATE_DESCRIPTION = RATE_DESCRIPTION + CREATE_TASK_DESCRIPTION;
    private static string CLOSE_TASK_DESCRIPTION = "closed tasks";
    private static string CLOSE_TASK_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + CLOSE_TASK_DESCRIPTION;
    private static string CLOSE_TASK_RATE_DESCRIPTION = RATE_DESCRIPTION + CLOSE_TASK_DESCRIPTION;
    private static string POLL_DESCRIPTION = "poll calls";
    private static string POLL_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + POLL_DESCRIPTION;
    private static string POLL_RATE_DESCRIPTION = RATE_DESCRIPTION + POLL_DESCRIPTION;
    private static string PROCESS_DESCRIPTION = "process calls";
    private static string PROCESS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PROCESS_DESCRIPTION;
    private static string PROCESS_RATE_DESCRIPTION = RATE_DESCRIPTION + PROCESS_DESCRIPTION;
    private static string PUNCTUATE_DESCRIPTION = "punctuate calls";
    private static string PUNCTUATE_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static string PUNCTUATE_RATE_DESCRIPTION = RATE_DESCRIPTION + PUNCTUATE_DESCRIPTION;
    private static string SKIP_RECORDS_DESCRIPTION = "skipped records";
    private static string SKIP_RECORD_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + SKIP_RECORDS_DESCRIPTION;
    private static string SKIP_RECORD_RATE_DESCRIPTION = RATE_DESCRIPTION + SKIP_RECORDS_DESCRIPTION;
    private static string COMMIT_OVER_TASKS_DESCRIPTION = "commit calls over all tasks";
    private static string COMMIT_OVER_TASKS_TOTAL_DESCRIPTION = TOTAL_DESCRIPTION + COMMIT_OVER_TASKS_DESCRIPTION;
    private static string COMMIT_OVER_TASKS_RATE_DESCRIPTION = RATE_DESCRIPTION + COMMIT_OVER_TASKS_DESCRIPTION;

    private static string COMMIT_LATENCY = COMMIT + LATENCY_SUFFIX;
    private static string POLL_LATENCY = POLL + LATENCY_SUFFIX;
    private static string PROCESS_LATENCY = PROCESS + LATENCY_SUFFIX;
    private static string PUNCTUATE_LATENCY = PUNCTUATE + LATENCY_SUFFIX;

    public static Sensor createTaskSensor(StreamsMetricsImpl streamsMetrics)
{
        Sensor createTaskSensor = streamsMetrics.threadLevelSensor(CREATE_TASK, RecordingLevel.INFO);
        addInvocationRateAndCount(createTaskSensor,
                                  THREAD_LEVEL_GROUP,
                                  streamsMetrics.threadLevelTagMap(),
                                  CREATE_TASK,
                                  CREATE_TASK_TOTAL_DESCRIPTION,
                                  CREATE_TASK_RATE_DESCRIPTION);
        return createTaskSensor;
    }

    public static Sensor closeTaskSensor(StreamsMetricsImpl streamsMetrics)
{
        Sensor closeTaskSensor = streamsMetrics.threadLevelSensor(CLOSE_TASK, RecordingLevel.INFO);
        addInvocationRateAndCount(closeTaskSensor,
                                  THREAD_LEVEL_GROUP,
                                  streamsMetrics.threadLevelTagMap(),
                                  CLOSE_TASK,
                                  CLOSE_TASK_TOTAL_DESCRIPTION,
                                  CLOSE_TASK_RATE_DESCRIPTION);
        return closeTaskSensor;
    }

    public static Sensor commitSensor(StreamsMetricsImpl streamsMetrics)
{
        Sensor commitSensor = streamsMetrics.threadLevelSensor(COMMIT, RecordingLevel.INFO);
        Dictionary<string, string> tagMap = streamsMetrics.threadLevelTagMap();
        addAvgAndMax(commitSensor, THREAD_LEVEL_GROUP, tagMap, COMMIT_LATENCY);
        addInvocationRateAndCount(commitSensor,
                                  THREAD_LEVEL_GROUP,
                                  tagMap,
                                  COMMIT,
                                  COMMIT_TOTAL_DESCRIPTION,
                                  COMMIT_RATE_DESCRIPTION);
        return commitSensor;
    }

    public static Sensor pollSensor(StreamsMetricsImpl streamsMetrics)
{
        Sensor pollSensor = streamsMetrics.threadLevelSensor(POLL, RecordingLevel.INFO);
        Dictionary<string, string> tagMap = streamsMetrics.threadLevelTagMap();
        addAvgAndMax(pollSensor, THREAD_LEVEL_GROUP, tagMap, POLL_LATENCY);
        addInvocationRateAndCount(pollSensor,
                                  THREAD_LEVEL_GROUP,
                                  tagMap,
                                  POLL,
                                  POLL_TOTAL_DESCRIPTION,
                                  POLL_RATE_DESCRIPTION);
        return pollSensor;
    }

    public static Sensor processSensor(StreamsMetricsImpl streamsMetrics)
{
        Sensor processSensor = streamsMetrics.threadLevelSensor(PROCESS, RecordingLevel.INFO);
        Dictionary<string, string> tagMap = streamsMetrics.threadLevelTagMap();
        addAvgAndMax(processSensor, THREAD_LEVEL_GROUP, tagMap, PROCESS_LATENCY);
        addInvocationRateAndCount(processSensor,
                                  THREAD_LEVEL_GROUP,
                                  tagMap,
                                  PROCESS,
                                  PROCESS_TOTAL_DESCRIPTION,
                                  PROCESS_RATE_DESCRIPTION);

        return processSensor;
    }

    public static Sensor punctuateSensor(StreamsMetricsImpl streamsMetrics)
{
        Sensor punctuateSensor = streamsMetrics.threadLevelSensor(PUNCTUATE, RecordingLevel.INFO);
        Dictionary<string, string> tagMap = streamsMetrics.threadLevelTagMap();
        addAvgAndMax(punctuateSensor, THREAD_LEVEL_GROUP, tagMap, PUNCTUATE_LATENCY);
        addInvocationRateAndCount(punctuateSensor,
                                  THREAD_LEVEL_GROUP,
                                  tagMap,
                                  PUNCTUATE,
                                  PUNCTUATE_TOTAL_DESCRIPTION,
                                  PUNCTUATE_RATE_DESCRIPTION);

        return punctuateSensor;
    }

    public static Sensor skipRecordSensor(StreamsMetricsImpl streamsMetrics)
{
        Sensor skippedRecordsSensor = streamsMetrics.threadLevelSensor(SKIP_RECORD, RecordingLevel.INFO);
        addInvocationRateAndCount(skippedRecordsSensor,
                                  THREAD_LEVEL_GROUP,
                                  streamsMetrics.threadLevelTagMap(),
                                  SKIP_RECORD,
                                  SKIP_RECORD_TOTAL_DESCRIPTION,
                                  SKIP_RECORD_RATE_DESCRIPTION);

        return skippedRecordsSensor;
    }

    public static Sensor commitOverTasksSensor(StreamsMetricsImpl streamsMetrics)
{
        Sensor commitOverTasksSensor = streamsMetrics.threadLevelSensor(COMMIT, RecordingLevel.DEBUG);
        Dictionary<string, string> tagMap = streamsMetrics.threadLevelTagMap(TASK_ID_TAG, ALL_TASKS);
        addAvgAndMax(commitOverTasksSensor,
                     TASK_LEVEL_GROUP,
                     tagMap,
                     COMMIT_LATENCY);
        addInvocationRateAndCount(commitOverTasksSensor,
                                  TASK_LEVEL_GROUP,
                                  tagMap,
                                  COMMIT,
                                  COMMIT_OVER_TASKS_TOTAL_DESCRIPTION,
                                  COMMIT_OVER_TASKS_RATE_DESCRIPTION);

        return commitOverTasksSensor;
    }
}
