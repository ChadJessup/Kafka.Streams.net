
namespace Kafka.Common.Metrics
{
    /**
     * A measurable quantity that can be registered as a metric
     */
    public interface IMeasurable : IMetricValueProvider<double>, IMetricValueProvider
    {
        /**
         * Measure this quantity and return the result as a double
         * @param config The configuration for this metric
         * @param now The POSIX time in milliseconds the measurement is being taken
         * @return The measured value
         */
        double measure(MetricConfig config, long now);
    }
}