namespace Kafka.Common.Metrics
{
    /**
     * Super-interface for {@link Measurable} or {@link Gauge} that provides
     * metric values.
     * <p>
     * In the future for Java8 and above, {@link Gauge#value(MetricConfig, long)} will be
     * moved to this interface with a default implementation in {@link Measurable} that returns
     * {@link Measurable#measure(MetricConfig, long)}.
     * </p>
     */
    public interface IMetricValueProvider
    {

    }
    public interface IMetricValueProvider<T>
    {
    }
}