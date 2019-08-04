
namespace Kafka.Common.Metrics.Stats
{
    /**
     * An instantaneous value.
     */
    public Value : IMeasurableStat
    {
        private double value = 0;
        public double measure(MetricConfig config, long now)
        {
            return value;
        }

        public void record(MetricConfig config, double value, long timeMs)
        {
            this.value = value;
        }
    }
}
