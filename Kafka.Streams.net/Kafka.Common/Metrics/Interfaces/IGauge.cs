namespace Kafka.Common.Metrics
{
    /**
     * A gauge metric is an instantaneous reading of a particular value.
     */
    public interface IGauge
    {
        object value(MetricConfig config, long now);
    }

    public interface IGauge<T> : IGauge, IMetricValueProvider<T>, IMetricValueProvider
    {
        /**
         * Returns the current value associated with this gauge.
         * @param config The configuration for this metric
         * @param now The POSIX time in milliseconds the measurement is being taken
         */
        T value(MetricConfig config, long now);
    }
}