namespace Kafka.Common.Metrics
{
    /**
     * A MeasurableStat is a {@link Stat} that is also {@link Measurable} (i.e. can produce a single floating point value).
     * This is the interface used for most of the simple statistics such as {@link org.apache.kafka.common.metrics.stats.Avg},
     * {@link org.apache.kafka.common.metrics.stats.Max}, {@link org.apache.kafka.common.metrics.stats.CumulativeCount}, etc.
     */
    public interface IMeasurableStat : IStat, IMeasurable
    {

    }
}