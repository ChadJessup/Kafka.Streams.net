using Kafka.Common;
using Kafka.Common.Metrics;
using Kafka.Common.Metrics.Stats;
using Kafka.Streams.Processors.Internals.Metrics;
using Kafka.Streams.Tasks;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class TaskMetrics
    {
        readonly StreamsMetricsImpl metrics;
        public Sensor taskCommitTimeSensor { get; }
        public Sensor taskEnforcedProcessSensor { get; }
        private readonly string taskName;

        public TaskMetrics(TaskId id, StreamsMetricsImpl metrics)
        {
            taskName = id.ToString();
            this.metrics = metrics;
            string group = "stream-task-metrics";

            // first.Add the global operation metrics if not yet, with the global tags only
            //Sensor parent = ThreadMetrics.commitOverTasksSensor(metrics);

            // add the operation metrics with.Additional tags
            Dictionary<string, string> tagMap = metrics.tagMap("task-id", taskName);
            //taskCommitTimeSensor = metrics.taskLevelSensor(taskName, "commit", RecordingLevel.DEBUG, parent);
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
//            taskEnforcedProcessSensor = metrics.taskLevelSensor(taskName, "enforced-processing", RecordingLevel.DEBUG, parent);
            taskEnforcedProcessSensor.Add(
                    new MetricName("enforced-processing-rate", group, "The average number of occurrence of enforced-processing operation per second.", tagMap),
                    new Rate(TimeUnit.SECONDS, new WindowedCount())
            );
            taskEnforcedProcessSensor.Add(
                    new MetricName("enforced-processing-total", group, "The total number of occurrence of enforced-processing operations.", tagMap),
                    new CumulativeCount()
            );

        }

        public void removeAllSensors()
        {
            metrics.removeAllTaskLevelSensors(taskName);
        }
    }
}