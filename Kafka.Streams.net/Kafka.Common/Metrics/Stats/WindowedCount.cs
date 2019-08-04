namespace Kafka.Common.Metrics.Stats
{
    /**
     * A {@link SampledStat} that maintains a simple count of what it has seen.
     * This is a special kind of {@link WindowedSum} that always records a value of {@code 1} instead of the provided value.
     * In other words, it counts the number of
     * {@link WindowedCount#record(MetricConfig, double, long)} invocations,
     * instead of summing the recorded values.
     *
     * See also {@link CumulativeCount} for a non-sampled version of this metric.
     */
    public WindowedCount : WindowedSum
    {
        protected override void update(Sample sample, MetricConfig config, double value, long now)
{
            base.update(sample, config, 1.0, now);
        }
    }
}