namespace Kafka.Common.Metrics.Stats
{
    /**
     * A non-sampled version of {@link WindowedCount} maintained over all time.
     *
     * This is a special kind of {@link CumulativeSum} that always records {@code 1} instead of the provided value.
     * In other words, it counts the number of
     * {@link CumulativeCount#record(MetricConfig, double, long)} invocations,
     * instead of summing the recorded values.
     */
    public CumulativeCount : CumulativeSum
    {
        public override void record(MetricConfig config, double value, long timeMs)
        {
            base.record(config, 1, timeMs);
        }
    }
}