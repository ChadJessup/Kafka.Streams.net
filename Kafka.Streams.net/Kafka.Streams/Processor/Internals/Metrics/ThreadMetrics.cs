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
using Kafka.Common.Metrics;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals.Metrics
{
    public class ThreadMetrics
    {
        private ThreadMetrics() { }

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

        private static string COMMIT_LATENCY = COMMIT + StreamsMetricsImpl.LATENCY_SUFFIX;
        private static string POLL_LATENCY = POLL + StreamsMetricsImpl.LATENCY_SUFFIX;
        private static string PROCESS_LATENCY = PROCESS + StreamsMetricsImpl.LATENCY_SUFFIX;
        private static string PUNCTUATE_LATENCY = PUNCTUATE + StreamsMetricsImpl.LATENCY_SUFFIX;

        public static Sensor createTaskSensor(StreamsMetricsImpl streamsMetrics)
        {
            Sensor createTaskSensor = streamsMetrics.threadLevelSensor(CREATE_TASK, RecordingLevel.INFO);
            streamsMetrics.addInvocationRateAndCount(
                createTaskSensor,
                StreamsMetricsImpl.THREAD_LEVEL_GROUP,
                streamsMetrics.threadLevelTagMap(),
                CREATE_TASK,
                CREATE_TASK_TOTAL_DESCRIPTION,
                CREATE_TASK_RATE_DESCRIPTION);

            return createTaskSensor;
        }

        public static Sensor closeTaskSensor(StreamsMetricsImpl streamsMetrics)
        {
            Sensor closeTaskSensor = streamsMetrics.threadLevelSensor(CLOSE_TASK, RecordingLevel.INFO);
            streamsMetrics.addInvocationRateAndCount(
                closeTaskSensor,
                StreamsMetricsImpl.THREAD_LEVEL_GROUP,
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
            streamsMetrics.addAvgAndMax(
                commitSensor,
                StreamsMetricsImpl.THREAD_LEVEL_GROUP,
                tagMap,
                COMMIT_LATENCY);

            streamsMetrics.addInvocationRateAndCount(
                commitSensor,
                StreamsMetricsImpl.THREAD_LEVEL_GROUP,
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
            streamsMetrics.addAvgAndMax(pollSensor, StreamsMetricsImpl.THREAD_LEVEL_GROUP, tagMap, POLL_LATENCY);
            streamsMetrics.addInvocationRateAndCount(
                pollSensor,
                StreamsMetricsImpl.THREAD_LEVEL_GROUP,
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
            streamsMetrics.addAvgAndMax(processSensor, StreamsMetricsImpl.THREAD_LEVEL_GROUP, tagMap, PROCESS_LATENCY);
            streamsMetrics.addInvocationRateAndCount(
                processSensor,
                StreamsMetricsImpl.THREAD_LEVEL_GROUP,
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
            streamsMetrics.addAvgAndMax(punctuateSensor, StreamsMetricsImpl.THREAD_LEVEL_GROUP, tagMap, PUNCTUATE_LATENCY);
            streamsMetrics.addInvocationRateAndCount(
                punctuateSensor,
                StreamsMetricsImpl.THREAD_LEVEL_GROUP,
                tagMap,
                PUNCTUATE,
                PUNCTUATE_TOTAL_DESCRIPTION,
                PUNCTUATE_RATE_DESCRIPTION);

            return punctuateSensor;
        }

        public static Sensor skipRecordSensor(StreamsMetricsImpl streamsMetrics)
        {
            Sensor skippedRecordsSensor = streamsMetrics.threadLevelSensor(SKIP_RECORD, RecordingLevel.INFO);
            streamsMetrics.addInvocationRateAndCount(
                skippedRecordsSensor,
                StreamsMetricsImpl.THREAD_LEVEL_GROUP,
                streamsMetrics.threadLevelTagMap(),
                SKIP_RECORD,
                SKIP_RECORD_TOTAL_DESCRIPTION,
                SKIP_RECORD_RATE_DESCRIPTION);

            return skippedRecordsSensor;
        }

        public static Sensor commitOverTasksSensor(StreamsMetricsImpl streamsMetrics)
        {
            Sensor commitOverTasksSensor = streamsMetrics.threadLevelSensor(COMMIT, RecordingLevel.DEBUG);
            Dictionary<string, string> tagMap = streamsMetrics.threadLevelTagMap(StreamsMetricsImpl.TASK_ID_TAG, StreamsMetricsImpl.ALL_TASKS);
            streamsMetrics.addAvgAndMax(
                commitOverTasksSensor,
                StreamsMetricsImpl.TASK_LEVEL_GROUP,
                tagMap,
                COMMIT_LATENCY);

            streamsMetrics.addInvocationRateAndCount(
                commitOverTasksSensor,
                StreamsMetricsImpl.TASK_LEVEL_GROUP,
                tagMap,
                COMMIT,
                COMMIT_OVER_TASKS_TOTAL_DESCRIPTION,
                COMMIT_OVER_TASKS_RATE_DESCRIPTION);

            return commitOverTasksSensor;
        }
    }
}