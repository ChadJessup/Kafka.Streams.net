namespace Kafka.Common.Metrics.Stats
{
    /**
     * Definition of a frequency metric used in a {@link Frequencies} compound statistic.
     */
    public class Frequency
    {
        /**
         * Get the value of this metrics center point.
         *
         * @return the center point value
         */
        public double centerValue { get; }
        public MetricName name { get; }

        /**
         * Create an instance with the given name and center point value.
         *
         * @param name        the name of the frequency metric; may not be null
         * @param centerValue the value identifying the {@link Frequencies} bucket to be reported
         */
        public Frequency(MetricName name, double centerValue)
        {
            this.name = name;
            this.centerValue = centerValue;
        }
    }
}
