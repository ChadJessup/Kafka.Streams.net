
//using Kafka.Common;
//using Kafka.Common.Metrics;
//using Kafka.Common.Metrics.Stats;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.Processors.Internals.Metrics;
//using System.Collections.Generic;

//namespace Kafka.Streams.KStream.Internals.Metrics
//{
//    public class Sensors
//    {

//        private Sensors() { }

//        public static Sensor lateRecordDropSensor(IInternalProcessorContext<K, V>  context)
//        {
//            StreamsMetricsImpl metrics = context.metrics;
//            Sensor sensor = metrics.nodeLevelSensor(
//               context.taskId.ToString(),
//               context.currentNode().Name,
//               LATE_RECORD_DROP,
//               RecordingLevel.INFO
//           );
//            StreamsMetricsImpl.AddInvocationRateAndCount(
//                sensor,
//                PROCESSOR_NODE_METRICS_GROUP,
//                metrics.tagMap("task-id", context.taskId.ToString(), PROCESSOR_NODE_ID_TAG, context.currentNode().Name),
//                LATE_RECORD_DROP
//            );
//            return sensor;
//        }

//        public static Sensor recordLatenessSensor(IInternalProcessorContext<K, V>  context)
//        {
//            StreamsMetricsImpl metrics = context.metrics;

//            Sensor sensor = metrics.taskLevelSensor(
//               context.taskId.ToString(),
//               "record-lateness",
//               RecordingLevel.DEBUG
//           );

//            Dictionary<string, string> tags = metrics.tagMap(
//               "task-id", context.taskId.ToString()
//           );
//            sensor.Add(
//                new MetricName(
//                    "record-lateness-avg",
//                    "stream-task-metrics",
//                    "The average observed lateness of records.",
//                    tags),
//                new Avg()
//            );
//            sensor.Add(
//                new MetricName(
//                    "record-lateness-max",
//                    "stream-task-metrics",
//                    "The max observed lateness of records.",
//                    tags),
//                new Max()
//            );
//            return sensor;
//        }

//        public static Sensor suppressionEmitSensor<K, V>(IInternalProcessorContext<K, V> context)
//        {
//            StreamsMetricsImpl metrics = (StreamsMetricsImpl)context.metrics;

//            Sensor sensor = metrics.nodeLevelSensor(
//               context.taskId.ToString(),
//               context.currentNode().Name,
//               "suppression-emit",
//               RecordingLevel.DEBUG
//           );

//            Dictionary<string, string> tags = metrics.tagMap(
//               "task-id", context.taskId.ToString(),
//               PROCESSOR_NODE_ID_TAG, context.currentNode().Name
//           );

//            sensor.Add(
//                new MetricName(
//                    "suppression-emit-rate",
//                    PROCESSOR_NODE_METRICS_GROUP,
//                    "The average number of occurrence of suppression-emit operation per second.",
//                    tags
//                ),
//                new Rate(TimeUnit.SECONDS, new WindowedSum())
//            );
//            sensor.Add(
//                new MetricName(
//                    "suppression-emit-total",
//                    PROCESSOR_NODE_METRICS_GROUP,
//                    "The total number of occurrence of suppression-emit operations.",
//                    tags
//                ),
//                new CumulativeSum()
//            );
//            return sensor;
//        }
//    }
//}