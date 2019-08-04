using System;

namespace Kafka.Common.Metrics
{
    /**
     * Thrown when a sensor records a value that causes a metric to go outside the bounds configured as its quota
     */
    public QuotaViolationException : Exception
    {
        public QuotaViolationException(MetricName metricName, double value, double bound)
            : base($"'{metricName}' violated quota. Actual: {value}, Threshold: {bound}")
        {
            this.metricName = metricName;
            this.value = value;
            this.bound = bound;
        }

        public MetricName metricName { get; }

        public double value { get; }

        public double bound { get; }
    }
}