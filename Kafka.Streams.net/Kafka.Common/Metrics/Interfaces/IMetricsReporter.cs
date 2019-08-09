using Kafka.Common.Interfaces;
using Kafka.Common.Metrics;
using System;
using System.Collections.Generic;

namespace Kafka.Common.Metrics.Interfaces
{
    /**
     * A plugin interface to allow things to listen as new metrics are created so they can be reported.
     * <p>
     * Implement {@link org.apache.kafka.common.ClusterResourceListener} to receive cluster metadata once it's available. Please see the documentation for ClusterResourceListener for more information.
     */
    public interface IMetricsReporter : IConfigurable, IDisposable
    {
        /**
         * This is called when the reporter is first registered to initially register all existing metrics
         * @param metrics All currently existing metrics
         */
        void init(List<KafkaMetric> metrics);

        /**
         * This is called whenever a metric is updated or.Added
         * @param metric
         */
        void metricChange(KafkaMetric metric);

        /**
         * This is called whenever a metric is removed
         * @param metric
         */
        void metricRemoval(KafkaMetric metric);

        /**
         * Called when the metrics repository is closed.
         */
        void close();
    }
}