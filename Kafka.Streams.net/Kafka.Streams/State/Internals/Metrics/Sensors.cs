
//namespace Kafka.Streams.State.Internals.Metrics
//{

//    public class Sensors
//    {

//        private Sensors() { }

//        public static Sensor createTaskAndStoreLatencyAndThroughputSensors(RecordingLevel level,
//                                                                           string operation,
//                                                                           StreamsMetricsImpl metrics,
//                                                                           string metricsGroup,
//                                                                           string taskName,
//                                                                           string storeName,
//                                                                           Dictionary<string, string> taskTags,
//                                                                           Dictionary<string, string> storeTags)
//        {
//            Sensor taskSensor = metrics.taskLevelSensor(taskName, operation, level);
//       addAvgMaxLatency(taskSensor, metricsGroup, taskTags, operation);
//       addInvocationRateAndCount(taskSensor, metricsGroup, taskTags, operation);
//            Sensor sensor = metrics.storeLevelSensor(taskName, storeName, operation, level, taskSensor);
//       addAvgMaxLatency(sensor, metricsGroup, storeTags, operation);
//       addInvocationRateAndCount(sensor, metricsGroup, storeTags, operation);
//            return sensor;
//        }

//        public static Sensor createBufferSizeSensor(IStateStore store,
//                                                    IInternalProcessorContext<K, V>  context)
//        {
//            return getBufferSizeOrCountSensor(store, context, "size");
//        }

//        public static Sensor createBufferCountSensor(IStateStore store,
//                                                     IInternalProcessorContext<K, V>  context)
//        {
//            return getBufferSizeOrCountSensor(store, context, "count");
//        }

//        private static Sensor getBufferSizeOrCountSensor(IStateStore store,
//                                                         IInternalProcessorContext<K, V>  context,
//                                                         string property)
//        {
//            StreamsMetricsImpl metrics = context.metrics;

//            string sensorName = "suppression-buffer-" + property;

//            Sensor sensor = metrics.storeLevelSensor(
//                context.taskId().ToString(),
//                store.name,
//                sensorName,
//                RecordingLevel.DEBUG
//            );

//            string metricsGroup = "stream-buffer-metrics";

//            Dictionary<string, string> tags = metrics.tagMap(
//                "task-id", context.taskId().ToString(),
//                "buffer-id", store.name
//            );

//            sensor.Add(
//                new MetricName(
//                    sensorName + "-current",
//                    metricsGroup,
//                    "The current " + property + " of buffered records.",
//                    tags),
//                new Value()
//            );


//            sensor.Add(
//                new MetricName(
//                    sensorName + "-avg",
//                    metricsGroup,
//                    "The average " + property + " of buffered records.",
//                    tags),
//                new Avg()
//            );

//            sensor.Add(
//                new MetricName(
//                    sensorName + "-max",
//                    metricsGroup,
//                    "The max " + property + " of buffered records.",
//                    tags),
//                new Max()
//            );

//            return sensor;
//        }
//    }
//}