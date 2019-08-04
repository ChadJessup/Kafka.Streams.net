using Kafka.Common;
using Kafka.Common.Interfaces;
using Kafka.Common.Metrics;
using Kafka.Common.Metrics.Stats;
using Kafka.Streams.Processor.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Processor.Internals.Metrics
{
    public StreamsMetricsImpl : IStreamsMetrics
    {
        private Dictionary<Sensor, Sensor> parentSensors;
        public Dictionary<MetricName, IMetric> streamMetrics { get; }
        public MetricsRegistry metrics { get; }

        private string threadName;

        private List<string> threadLevelSensors = new List<string>();
        private Dictionary<string, List<string>> taskLevelSensors = new Dictionary<string, List<string>>();
        private Dictionary<string, List<string>> nodeLevelSensors = new Dictionary<string, List<string>>();
        private Dictionary<string, List<string>> cacheLevelSensors = new Dictionary<string, List<string>>();
        private Dictionary<string, List<string>> storeLevelSensors = new Dictionary<string, List<string>>();

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

        public StreamsMetricsImpl(MetricsRegistry metrics, string threadName)
        {
            this.metrics = metrics ?? throw new ArgumentNullException("Metrics cannot be null", nameof(metrics));
            this.threadName = threadName;

            this.parentSensors = new Dictionary<Sensor, Sensor>();
        }

        public Sensor threadLevelSensor(
            string sensorName,
            RecordingLevel recordingLevel,
            List<Sensor> parents)
        {

            lock (threadLevelSensors)
            {
                string fullSensorName = threadSensorPrefix() + SENSOR_NAME_DELIMITER + sensorName;
                Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);
                threadLevelSensors.Add(fullSensorName);
                return sensor;
            }
        }

        private string threadSensorPrefix()
        {
            return "internal" + SENSOR_PREFIX_DELIMITER + threadName;
        }

        public Dictionary<string, string> threadLevelTagMap()
        {
            Dictionary<string, string> tagMap = new Dictionary<string, string>();

            tagMap.Add(THREAD_ID_TAG, threadName);
            return tagMap;
        }

        public Dictionary<string, string> threadLevelTagMap(string[] tags)
        {
            Dictionary<string, string> tagMap = threadLevelTagMap();
            if (tags != null)
            {
                if ((tags.Length % 2) != 0)
                {
                    throw new System.ArgumentException("Tags needs to be specified in key-value pairs");
                }

                for (int i = 0; i < tags.Length; i += 2)
                {
                    tagMap.Add(tags[i], tags[i + 1]);
                }
            }
            return tagMap;
        }

        public void removeAllThreadLevelSensors()
        {
            lock (threadLevelSensors)
            {
                while (threadLevelSensors.Any())
                {
                    var sensor = threadLevelSensors[0];
                    threadLevelSensors.RemoveAt(0);

                    metrics.removeSensor(sensor);
                }
            }
        }

        public Sensor taskLevelSensor(
            string taskName,
            string sensorName,
            RecordingLevel recordingLevel,
            List<Sensor> parents)
        {
            string key = taskSensorPrefix(taskName);
            lock (taskLevelSensors)
            {
                if (!taskLevelSensors.ContainsKey(key))
                {
                    taskLevelSensors.Add(key, new List<string>());
                }

                string fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;

                Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

                taskLevelSensors[key].Add(fullSensorName);

                return sensor;
            }
        }

        public void removeAllTaskLevelSensors(string taskName)
        {
            string key = taskSensorPrefix(taskName);
            lock (taskLevelSensors)
            {
                List<string> sensors = taskLevelSensors[key];
                taskLevelSensors.Remove(key);

                while (sensors != null && sensors.Any())
                {
                    var sensor = sensors[0];
                    sensors.RemoveAt(0);
                    metrics.removeSensor(sensor);
                }
            }
        }

        private string taskSensorPrefix(string taskName)
        {
            return threadSensorPrefix() + SENSOR_PREFIX_DELIMITER + "task" + SENSOR_PREFIX_DELIMITER + taskName;
        }

        public Sensor nodeLevelSensor(
            string taskName,
            string processorNodeName,
            string sensorName,
            RecordingLevel recordingLevel,
            List<Sensor> parents)
        {
            string key = nodeSensorPrefix(taskName, processorNodeName);
            lock (nodeLevelSensors)
            {
                if (!nodeLevelSensors.ContainsKey(key))
                {
                    nodeLevelSensors.Add(key, new List<string>());
                }

                string fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;

                Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

                nodeLevelSensors[key].Add(fullSensorName);

                return sensor;
            }
        }

        public void removeAllNodeLevelSensors(string taskName, string processorNodeName)
        {
            string key = nodeSensorPrefix(taskName, processorNodeName);
            lock (nodeLevelSensors)
            {
                var sensors = nodeLevelSensors[key];
                nodeLevelSensors.Remove(key);

                while (sensors != null && sensors.Any())
                {
                    var sensor = sensors[0];
                    sensors.RemoveAt(0);
                    metrics.removeSensor(sensor);
                }
            }
        }

        private string nodeSensorPrefix(string taskName, string processorNodeName)
        {
            return taskSensorPrefix(taskName) + SENSOR_PREFIX_DELIMITER + "node" + SENSOR_PREFIX_DELIMITER + processorNodeName;
        }

        public Sensor cacheLevelSensor(
            string taskName,
            string cacheName,
            string sensorName,
            RecordingLevel recordingLevel,
            List<Sensor> parents)
        {
            string key = cacheSensorPrefix(taskName, cacheName);
            lock (cacheLevelSensors)
            {
                if (!cacheLevelSensors.ContainsKey(key))
                {
                    cacheLevelSensors.Add(key, new List<string>());
                }

                string fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;

                Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

                cacheLevelSensors[key].Add(fullSensorName);

                return sensor;
            }
        }

        public void removeAllCacheLevelSensors(string taskName, string cacheName)
        {
            string key = cacheSensorPrefix(taskName, cacheName);
            lock (cacheLevelSensors)
            {
                List<string> strings = cacheLevelSensors[key];
                cacheLevelSensors.Remove(key);

                while (strings != null && strings.Any())
                {
                    var top = strings[0];
                    strings.RemoveAt(0);
                    metrics.removeSensor(top);
                }
            }
        }

        private string cacheSensorPrefix(string taskName, string cacheName)
        {
            return taskSensorPrefix(taskName) + SENSOR_PREFIX_DELIMITER + "cache" + SENSOR_PREFIX_DELIMITER + cacheName;
        }

        public Sensor storeLevelSensor(
            string taskName,
            string storeName,
            string sensorName,
            RecordingLevel recordingLevel,
            List<Sensor> parents)
        {
            string key = storeSensorPrefix(taskName, storeName);
            lock (storeLevelSensors)
            {
                if (!storeLevelSensors.ContainsKey(key))
                {
                    storeLevelSensors.Add(key, new List<string>());
                }

                string fullSensorName = key + SENSOR_NAME_DELIMITER + sensorName;

                Sensor sensor = metrics.sensor(fullSensorName, recordingLevel, parents);

                storeLevelSensors[key].Add(fullSensorName);

                return sensor;
            }
        }

        public void removeAllStoreLevelSensors(string taskName, string storeName)
        {
            string key = storeSensorPrefix(taskName, storeName);
            lock (storeLevelSensors)
            {
                storeLevelSensors.TryGetValue(key, out var sensors);
                while (sensors != null && sensors.Any())
                {
                    var sensor = sensors[0];
                    sensors.Remove(sensor);
                    metrics.removeSensor(sensor);
                }
            }
        }

        private string storeSensorPrefix(string taskName, string storeName)
        {
            return taskSensorPrefix(taskName) + SENSOR_PREFIX_DELIMITER + "store" + SENSOR_PREFIX_DELIMITER + storeName;
        }


        public Sensor AddSensor(string name, RecordingLevel recordingLevel)
        {
            return metrics.sensor(name, recordingLevel);
        }

        public Sensor AddSensor(string name, RecordingLevel recordingLevel, List<Sensor> parents)
        {
            return metrics.sensor(name, recordingLevel, parents);
        }

        public void recordLatency(Sensor sensor, long startNs, long endNs)
        {
            sensor.record(endNs - startNs);
        }

        public void recordThroughput(Sensor sensor, long value)
        {
            sensor.record(value);
        }

        public Dictionary<string, string> tagMap(string[] tags)
        {
            var tagMap = new Dictionary<string, string>();
            tagMap.Add("client-id", threadName);
            if (tags != null)
            {
                if ((tags.Length % 2) != 0)
                {
                    throw new System.ArgumentException("Tags needs to be specified in key-value pairs");
                }

                for (int i = 0; i < tags.Length; i += 2)
                {
                    tagMap.Add(tags[i], tags[i + 1]);
                }
            }
            return tagMap;
        }


        private Dictionary<string, string> constructTags(string scopeName, string entityName, string[] tags)
        {
            string[] updatedTags = new string[tags.Length + 2];
            Array.Copy(tags, updatedTags, tags.Length + 2);
            updatedTags[tags.Length] = scopeName + "-id";
            updatedTags[tags.Length + 1] = entityName;

            return tagMap(updatedTags);
        }

        public void removeSensor(Sensor sensor)
        {
            throw new NotImplementedException();
        }


        /**
         * @throws ArgumentException if tags is not constructed in key-value pairs
         */

        public Sensor addLatencyAndThroughputSensor(
            string scopeName,
            string entityName,
            string operationName,
            RecordingLevel recordingLevel,
            string[] tags)
        {
            string group = groupNameFromScope(scopeName);

            Dictionary<string, string> tagMap = constructTags(scopeName, entityName, tags);
            Dictionary<string, string> allTagMap = constructTags(scopeName, "all", tags);

            // first.Add the global operation metrics if not yet, with the global tags only
            Sensor parent = metrics.sensor(externalParentSensorName(operationName), recordingLevel);
           addAvgMaxLatency(parent, group, allTagMap, operationName);
           addInvocationRateAndCount(parent, group, allTagMap, operationName);

            //.Add the operation metrics with.Additional tags
            Sensor sensor = metrics.sensor(externalChildSensorName(operationName, entityName), recordingLevel, new List<Sensor> { parent });
           addAvgMaxLatency(sensor, group, tagMap, operationName);
           addInvocationRateAndCount(sensor, group, tagMap, operationName);

            parentSensors.Add(sensor, parent);

            return sensor;
        }

        /**
         * @throws ArgumentException if tags is not constructed in key-value pairs
         */

        public Sensor addThroughputSensor(
            string scopeName,
            string entityName,
            string operationName,
            RecordingLevel recordingLevel,
            string[] tags)
        {
            string group = groupNameFromScope(scopeName);

            Dictionary<string, string> tagMap = constructTags(scopeName, entityName, tags);
            Dictionary<string, string> allTagMap = constructTags(scopeName, "all", tags);

            // first.Add the global operation metrics if not yet, with the global tags only
            Sensor parent = metrics.sensor(externalParentSensorName(operationName), recordingLevel);
            addInvocationRateAndCount(parent, group, allTagMap, operationName);

            //.Add the operation metrics with.Additional tags
            Sensor sensor = metrics.sensor(externalChildSensorName(operationName, entityName), recordingLevel, new List<Sensor> { parent });
            addInvocationRateAndCount(sensor, group, tagMap, operationName);

            parentSensors.Add(sensor, parent);

            return sensor;
        }

        private string externalChildSensorName(string operationName, string entityName)
        {
            return "external" + SENSOR_PREFIX_DELIMITER + threadName
                + SENSOR_PREFIX_DELIMITER + "entity" + SENSOR_PREFIX_DELIMITER + entityName
                + SENSOR_NAME_DELIMITER + operationName;
        }

        private string externalParentSensorName(string operationName)
        {
            return "external" + SENSOR_PREFIX_DELIMITER + threadName + SENSOR_NAME_DELIMITER + operationName;
        }

        public static void addAvgAndMax(
            Sensor sensor,
            string group,
            Dictionary<string, string> tags,
            string operation)
        {
            sensor.Add(
                new MetricName(
                    operation + AVG_SUFFIX,
                    group,
                    "The average value of " + operation + ".",
                    tags),
                new Avg());

            sensor.Add(
                new MetricName(
                    operation + MAX_SUFFIX,
                    group,
                    "The max value of " + operation + ".",
                    tags),
                new Max());
        }

        public static void addAvgMaxLatency(
            Sensor sensor,
            string group,
            Dictionary<string, string> tags,
            string operation)
        {
            sensor.Add(
                new MetricName(
                    operation + "-latency-avg",
                    group,
                    "The average latency of " + operation + " operation.",
                    tags),
                new Avg());

            sensor.Add(
                new MetricName(
                    operation + "-latency-max",
                    group,
                    "The max latency of " + operation + " operation.",
                    tags),
                new Max());
        }

        public static void addInvocationRateAndCount(
            Sensor sensor,
            string group,
            Dictionary<string, string> tags,
            string operation,
            string descriptionOfInvocation,
            string descriptionOfRate)
        {
            sensor.Add(
                new MetricName(
                    operation + TOTAL_SUFFIX,
                    group,
                    descriptionOfInvocation,
                    tags),
                new CumulativeCount());

            sensor.Add(
                new MetricName(
                    operation + RATE_SUFFIX,
                    group,
                    descriptionOfRate,
                    tags),
                new Rate(TimeUnit.SECONDS, new WindowedCount()));
        }

        public static void addInvocationRateAndCount(
            Sensor sensor,
            string group,
            Dictionary<string, string> tags,
            string operation)
        {
           addInvocationRateAndCount(
                sensor,
                group,
                tags,
                operation,
                "The total number of " + operation,
                "The average per-second number of " + operation);
        }

        /**
         * Deletes a sensor and its parents, if any
         */
        //public void removeSensor(Sensor sensor)
        //{
        //    //   sensor = sensor ?? throw new System.ArgumentNullException("Sensor is null", nameof(sensor));
        //    metrics.removeSensor(sensor.name);

        //    if (parentSensors.TryGetValue(sensor, out var parent))
        //    {
        //        metrics.removeSensor(parent.name);
        //    }
        //}

        private static string groupNameFromScope(string scopeName)
        {
            return "stream-" + scopeName + "-metrics";
        }

        public Sensor addSensor(string name, RecordingLevel recordingLevel, List<Sensor> parents)
        {
            throw new NotImplementedException();
        }
    }
}