namespace Kafka.Common.Metrics.Stats
{
    /**
     * A simple rate the rate is incrementally calculated
     * based on the elapsed time between the earliest reading
     * and now.
     *
     * An exception is made for the first window, which is
     * considered of fixed size. This avoids the issue of
     * an artificially high rate when the gap between readings
     * is close to 0.
     */
    public class SimpleRate : Rate
    {
        public override long windowSize(MetricConfig config, long now)
        {
            stat.purgeObsoleteSamples(config, now);
            long elapsed = now - stat.oldest(now).lastWindowMs;
            return elapsed < config.timeWindowMs ? config.timeWindowMs : elapsed;
        }
    }
}
