namespace Kafka.Common.Metrics.Stats
{
    /**
     * An non-sampled cumulative total maintained over all time.
     * This is a non-sampled version of {@link WindowedSum}.
     *
     * See also {@link CumulativeCount} if you just want to increment the value by 1 on each recording.
     */
    public CumulativeSum : IMeasurableStat
    {
        private double total;

        public CumulativeSum()
        {
            total = 0.0;
        }

        public CumulativeSum(double value)
        {
            total = value;
        }

        public virtual void record(MetricConfig config, double value, long now)
        {
            total += value;
        }

        public double measure(MetricConfig config, long now)
        {
            return total;
        }
    }
}