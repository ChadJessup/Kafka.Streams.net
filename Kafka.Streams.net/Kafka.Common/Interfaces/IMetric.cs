namespace Kafka.Common.Interfaces
{
    /**
     * A metric tracked for monitoring purposes.
     */
    public interface IMetric
    {

        /**
         * A name for this metric
         */
        MetricName metricName();

        /**
         * The value of the metric, which may be measurable or a non-measurable gauge
         */
        object metricValue();

    }
}