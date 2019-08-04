﻿using Kafka.Common.Metrics;
using Kafka.Streams.Processor.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class NodeMetrics
    {
        private StreamsMetricsImpl metrics;

        private Sensor nodeProcessTimeSensor;
        private Sensor nodePunctuateTimeSensor;
        private Sensor sourceNodeForwardSensor;
        private Sensor nodeCreationSensor;
        private Sensor nodeDestructionSensor;
        private string taskName;
        private string processorNodeName;

        private NodeMetrics(
            StreamsMetricsImpl metrics,
            string processorNodeName,
            IProcessorContext context)
        {
            this.metrics = metrics;

            string taskName = context.taskId().ToString();
            Dictionary<string, string> tagMap = metrics.tagMap("task-id", context.taskId().ToString(), PROCESSOR_NODE_ID_TAG, processorNodeName);
            Dictionary<string, string> allTagMap = metrics.tagMap("task-id", context.taskId().ToString(), PROCESSOR_NODE_ID_TAG, "all");

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

        private void removeAllSensors()
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
            addAvgMaxLatency(parent, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);
            addInvocationRateAndCount(parent, PROCESSOR_NODE_METRICS_GROUP, taskTags, operation);

            Sensor sensor = metrics.nodeLevelSensor(taskName, processorNodeName, operation, RecordingLevel.DEBUG, parent);
            addAvgMaxLatency(sensor, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);
            addInvocationRateAndCount(sensor, PROCESSOR_NODE_METRICS_GROUP, nodeTags, operation);

            return sensor;
        }
    }
}