using Kafka.Common;
using Kafka.Common.Interfaces;
using Kafka.Common.Metrics;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Interfaces
{
    public interface IStreamsMetrics
    {
        /**
         * Get read-only handle on global metrics registry.
         *
         * @return Map of all metrics.
         */
        Dictionary<MetricName, IMetric> streamMetrics { get; }

        /**
         * Add a latency and throughput sensor for a specific operation, which will include the following sensors:
         * <ol>
         *   <li>average latency</li>
         *   <li>max latency</li>
         *   <li>throughput (num.operations / time unit)</li>
         * </ol>
         * Also create a parent sensor with the same metrics that aggregates all entities with the same operation under the
         * same scope if it has not been created.
         *
         * @param scopeName      name of the scope, could be the type of the state store, etc.
         * @param entityName     name of the entity, could be the name of the state store instance, etc.
         * @param operationName  name of the operation, could be get / put / delete / etc.
         * @param recordingLevel the recording level (e.g., INFO or DEBUG) for this sensor.
         * @param tags           additional tags of the sensor
         * @return The added sensor.
         */
        Sensor addLatencyAndThroughputSensor(
            string scopeName,
            string entityName,
            string operationName,
            RecordingLevel recordingLevel,
            string[] tags];

        /**
         * Record the given latency value of the sensor.
         * If the passed sensor includes throughput metrics, e.g., when created by the
         * {@link #addLatencyAndThroughputSensor(string, string, string, RecordingLevel, string...)} method, then the
         * throughput metrics will also be recorded from this event.
         *
         * @param sensor  sensor whose latency we are recording.
         * @param startNs start of measurement time in nanoseconds.
         * @param endNs   end of measurement time in nanoseconds.
         */
        void recordLatency(
            Sensor sensor,
            long startNs,
            long endNs);

        /**
         * Add a throughput sensor for a specific operation:
         * <ol>
         *   <li>throughput (num.operations / time unit)</li>
         * </ol>
         * Also create a parent sensor with the same metrics that aggregates all entities with the same operation under the
         * same scope if it has not been created.
         * This sensor is a strict subset of the sensors created by
         * {@link #addLatencyAndThroughputSensor(string, string, string, RecordingLevel, string...)}.
         *
         * @param scopeName      name of the scope, could be the type of the state store, etc.
         * @param entityName     name of the entity, could be the name of the state store instance, etc.
         * @param operationName  name of the operation, could be get / put / delete / etc.
         * @param recordingLevel the recording level (e.g., INFO or DEBUG) for this sensor.
         * @param tags           additional tags of the sensor
         * @return The added sensor.
         */
        Sensor addThroughputSensor(
            string scopeName,
            string entityName,
            string operationName,
            RecordingLevel recordingLevel,
            string[] tags];

        /**
         * Record the throughput value of a sensor.
         *
         * @param sensor add Sensor whose throughput we are recording
         * @param value  throughput value
         */
        void recordThroughput(
            Sensor sensor,
            long value);


        /**
         * Generic method to create a sensor.
         * Note that for most cases it is advisable to use
         * {@link #addThroughputSensor(string, string, string, RecordingLevel, string...)}
         * or {@link #addLatencyAndThroughputSensor(string, string, string, RecordingLevel, string...)} to ensure
         * metric name well-formedness and conformity with the rest of the streams code base.
         * However, if the above two methods are not sufficient, this method can also be used.
         *
         * @param name           name of the sensor.
         * @param recordingLevel the recording level (e.g., INFO or DEBUG) for this sensor
         * @return The added sensor.
         */
        Sensor addSensor(
            string name,
            RecordingLevel recordingLevel);

        /**
         * Generic method to create a sensor with parent sensors.
         * Note that for most cases it is advisable to use
         * {@link #addThroughputSensor(string, string, string, RecordingLevel, string...)}
         * or {@link #addLatencyAndThroughputSensor(string, string, string, RecordingLevel, string...)} to ensure
         * metric name well-formedness and conformity with the rest of the streams code base.
         * However, if the above two methods are not sufficient, this method can also be used.
         *
         * @param name           name of the sensor
         * @param recordingLevel the recording level (e.g., INFO or DEBUG) for this sensor
         * @return The added sensor.
         */
        Sensor addSensor(
            string name,
            RecordingLevel recordingLevel,
            Sensor[] parents];

        /**
         * Remove a sensor.
         * @param sensor sensor to be removed
         */
        void removeSensor(Sensor sensor);
    }
}