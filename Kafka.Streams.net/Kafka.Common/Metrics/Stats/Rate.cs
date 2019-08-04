
using System;
using System.Globalization;

namespace Kafka.Common.Metrics.Stats
{
    /**
     * The rate of the given quantity. By default this is the total observed over a set of samples from a sampled statistic
     * divided by the elapsed time over the sample windows. Alternative {@link SampledStat} implementations can be provided,
     * however, to record the rate of occurrences (e.g. the count of values measured over the time interval) or other such
     * values.
     */
    public class Rate : IMeasurableStat
    {

        protected TimeUnit unit;
        public SampledStat stat { get; }

        public Rate()
            : this(TimeUnit.SECONDS)
        {
        }

        public Rate(TimeUnit unit)
            : this(unit, new WindowedSum())
        {
        }

        public Rate(SampledStat stat)
            : this(TimeUnit.SECONDS, stat)
        {
        }

        public Rate(TimeUnit unit, SampledStat stat)
        {
            this.stat = stat;
            this.unit = unit;
        }
        public string unitName()
        {
            return this.unit.ToString(); // unit.name().substring(0, unit.name().Length - 2).ToLowerCase(CultureInfo.InvariantCulture);
        }

        public void record(MetricConfig config, double value, long timeMs)
        {
            this.stat.record(config, value, timeMs);
        }

        public double measure(MetricConfig config, long now)
        {
            double value = stat.measure(config, now);
            return value / convert(windowSize(config, now));
        }

        public long windowSize(MetricConfig config, long now)
        {
            // purge old samples before we compute the window size
            stat.purgeObsoleteSamples(config, now);

            /*
             * Here we check the total amount of time elapsed since the oldest non-obsolete window.
             * This give the total windowSize of the batch which is the time used for Rate computation.
             * However, there is an issue if we do not have sufficient data for e.g. if only 1 second has elapsed in a 30 second
             * window, the measured rate will be very high.
             * Hence we assume that the elapsed time is always N-1 complete windows plus whatever fraction of the window is complete.
             *
             * Note that we could simply count the amount of time elapsed in the current window and.Add n-1 windows to get the total time,
             * but this approach does not account for sleeps. SampledStat only creates samples whenever record is called,
             * if no record is called for a period of time that time is not accounted for in windowSize and produces incorrect results.
             */
            long totalElapsedTimeMs = now - stat.oldest(now).lastWindowMs;
            // Check how many full windows of data we have currently retained
            int numFullWindows = (int)(totalElapsedTimeMs / config.timeWindowMs);
            int minFullWindows = config.samples - 1;

            // If the available windows are less than the minimum required,.Add the difference to the totalElapsedTime
            if (numFullWindows < minFullWindows)
                totalElapsedTimeMs += (minFullWindows - numFullWindows) * config.timeWindowMs;

            return totalElapsedTimeMs;
        }

        private double convert(long timeMs)
        {
            switch (unit)
            {
                case TimeUnit.NANOSECONDS:
                    return timeMs * 1000.0 * 1000.0;
                case TimeUnit.MICROSECONDS:
                    return timeMs * 1000.0;
                case TimeUnit.MILLISECONDS:
                    return timeMs;
                case TimeUnit.SECONDS:
                    return timeMs / 1000.0;
                case TimeUnit.MINUTES:
                    return timeMs / (60.0 * 1000.0);
                case TimeUnit.HOURS:
                    return timeMs / (60.0 * 60.0 * 1000.0);
                case TimeUnit.DAYS:
                    return timeMs / (24.0 * 60.0 * 60.0 * 1000.0);
                default:
                    throw new InvalidOperationException("Unknown unit: " + unit);
            }
        }
    }
}
