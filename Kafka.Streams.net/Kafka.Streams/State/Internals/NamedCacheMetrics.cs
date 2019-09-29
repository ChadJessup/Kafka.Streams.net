///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for.Additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//using System;
//using System.Collections.Generic;
//using Kafka.Common;
//using Kafka.Common.Metrics;
//using Kafka.Common.Metrics.Stats;
//using Kafka.Streams.Processors.Internals.Metrics;

//namespace Kafka.Streams.State.Internals
//{
//    public class NamedCacheMetrics
//    {
//        private StreamsMetricsImpl metrics;

//        private Sensor hitRatioSensor;
//        private string taskName;
//        private string cacheName;

//        public NamedCacheMetrics(StreamsMetricsImpl metrics, string cacheName)
//        {
//            //taskName = ThreadCache.taskIDfromCacheName(cacheName);
//            this.cacheName = cacheName;
//            this.metrics = metrics;
//            string group = "stream-record-cache-metrics";

//            //.Add parent
//            Dictionary<string, string> allMetricTags = metrics.tagMap(
//                 "task-id", taskName,
//                "record-cache-id", "all"
//            );
//            Sensor taskLevelHitRatioSensor = metrics.taskLevelSensor(taskName, "hitRatio", RecordingLevel.DEBUG);
//            taskLevelHitRatioSensor.Add(
//                new MetricName("hitRatio-avg", group, "The average cache hit ratio.", allMetricTags),
//                new Avg()
//            );
//            taskLevelHitRatioSensor.Add(
//                new MetricName("hitRatio-min", group, "The minimum cache hit ratio.", allMetricTags),
//                new Min()
//            );
//            taskLevelHitRatioSensor.Add(
//                new MetricName("hitRatio-max", group, "The maximum cache hit ratio.", allMetricTags),
//                new Max()
//            );

//            // add child
//            //Dictionary<string, string> metricTags = metrics.tagMap(
//            //     "task-id", taskName,
//            //    "record-cache-id", ThreadCache.underlyingStoreNamefromCacheName(cacheName)
//            //);

//            hitRatioSensor = metrics.cacheLevelSensor(
//                taskName,
//                cacheName,
//                "hitRatio",
//                RecordingLevel.DEBUG,
//                taskLevelHitRatioSensor
//            );
//            hitRatioSensor.Add(
//                new MetricName("hitRatio-avg", group, "The average cache hit ratio.", metricTags),
//                new Avg()
//            );
//            hitRatioSensor.Add(
//                new MetricName("hitRatio-min", group, "The minimum cache hit ratio.", metricTags),
//                new Min()
//            );
//            hitRatioSensor.Add(
//                new MetricName("hitRatio-max", group, "The maximum cache hit ratio.", metricTags),
//                new Max());
//        }

//        private void removeAllSensors()
//        {
//            metrics.removeAllCacheLevelSensors(taskName, cacheName);
//        }
//    }
//}
