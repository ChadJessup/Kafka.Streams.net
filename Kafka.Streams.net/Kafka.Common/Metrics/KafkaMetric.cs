using Kafka.Common;
using Kafka.Common.Interfaces;
using Kafka.Common.Metrics;
using Kafka.Common.Utils.Interfaces;
using System;
using System.Runtime.CompilerServices;

namespace Kakfa.Common.Metrics
{
    public class KafkaMetric : IMetric
    {
        private object @lock;
        private ITime time;
        private IMetricValueProvider metricValueProvider;

        // public for testing
        public KafkaMetric(
            object @lock,
            MetricName metricName,
            IMetricValueProvider valueProvider,
            MetricConfig config,
            ITime time)
        {
            this.metricName = metricName;
            this.@lock = @lock;
            if (!(valueProvider is IMeasurable) && !(valueProvider is IGauge))
            {
                throw new ArgumentException("Unsupported metric value provider of class " + valueProvider.GetType());
            }

            this.metricValueProvider = valueProvider;
            this.config = config;
            this.time = time;
        }

        public MetricConfig config { get; private set; }

        public MetricName metricName { get; }

        /**
         * See {@link Metric#value()} for the details on why this is deprecated.
         */
        [Obsolete)
        public double value()
{
            return measurableValue(time.milliseconds());
        }

        public object metricValue()
        {
            long now = time.milliseconds();

            lock (this.@lock)
            {
                if (this.metricValueProvider is IMeasurable)
                {
                    return ((IMeasurable)metricValueProvider).measure(config, now);
                }
                else if (this.metricValueProvider is IGauge)
                {
                    return ((IGauge)metricValueProvider).value(config, now);
                }
                else
                    throw new InvalidOperationException("Not a valid metric: " + this.metricValueProvider.GetType());
            }
        }

        public IMeasurable measurable()
        {
            if (this.metricValueProvider is IMeasurable)
                return (IMeasurable)metricValueProvider;
            else
                throw new InvalidOperationException("Not a measurable: " + this.metricValueProvider.GetType());
        }

        public double measurableValue(long timeMs)
        {
            lock (this.@lock)
            {
                if (this.metricValueProvider is IMeasurable)
                    return ((IMeasurable)metricValueProvider).measure(config, timeMs);
                else
                    return 0;
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Config(MetricConfig config)
{
            this.config = config;
        }
    }
}