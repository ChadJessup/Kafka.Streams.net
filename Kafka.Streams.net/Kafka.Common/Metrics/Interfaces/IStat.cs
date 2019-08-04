namespace Kafka.Common.Metrics
{
    /**
     * A Stat is a quantity such as average, max, etc that is computed off the stream of updates to a sensor
     */
    public interface IStat
    {
        /**
         * Record the given value
         * @param config The configuration to use for this metric
         * @param value The value to record
         * @param timeMs The POSIX time in milliseconds this value occurred
         */
        void record(MetricConfig config, double value, long timeMs);
    }
}