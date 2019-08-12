using Kafka.Common.Metrics;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals.Metrics;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class NodeMetrics<K, V>
    {
        private StreamsMetricsImpl metrics;

        public Sensor nodeProcessTimeSensor { get; }
        private Sensor nodePunctuateTimeSensor;
        private Sensor sourceNodeForwardSensor;
        private Sensor nodeCreationSensor;
        private Sensor nodeDestructionSensor;
        private string taskName;
        private string processorNodeName;

        public NodeMetrics(
            StreamsMetricsImpl metrics,
            string processorNodeName,
            IProcessorContext<K, V> context)
        {
            this.metrics = metrics;

            string taskName = context.taskId().ToString();
            Dictionary<string, string> tagMap = metrics.tagMap("task-id", context.taskId().ToString(), StreamsMetricsImpl.PROCESSOR_NODE_ID_TAG, processorNodeName);
            Dictionary<string, string> allTagMap = metrics.tagMap("task-id", context.taskId().ToString(), StreamsMetricsImpl.PROCESSOR_NODE_ID_TAG, "all");

            nodeProcessTimeSensor = createTaskAndNodeLatencyAndThroughputSensors(
                "process",
                metrics,
                taskName,
                processorNodeName,
                allTagMap,
                tagMap);

            nodePunctuateTimeSensor = createTaskAndNodeLatencyAndThroughputSensors(
                "punctuate",
                metrics,
                taskName,
                processorNodeName,
                allTagMap,
                tagMap);

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

        public void removeAllSensors()
        {
            metrics.removeAllNodeLevelSensors(taskName, processorNodeName);
        }

        private static Sensor createTaskAndNodeLatencyAndThroughputSensors(
            string operation,
            StreamsMetricsImpl metrics,
            string taskName,
            string processorNodeName,
            Dictionary<string, string> taskTags,
            Dictionary<string, string> nodeTags)
        {
            Sensor parent = metrics.taskLevelSensor(taskName, operation, RecordingLevel.DEBUG);
            StreamsMetricsImpl.addAvgMaxLatency(parent, StreamsMetricsImpl.PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);
            StreamsMetricsImpl.addInvocationRateAndCount(parent, StreamsMetricsImpl.PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);

            Sensor sensor = metrics.nodeLevelSensor(taskName, processorNodeName, operation, RecordingLevel.DEBUG, parent);
            StreamsMetricsImpl.addAvgMaxLatency(sensor, StreamsMetricsImpl.PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);
            StreamsMetricsImpl.addInvocationRateAndCount(sensor, StreamsMetricsImpl.PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);

            return sensor;
        }
    }
}