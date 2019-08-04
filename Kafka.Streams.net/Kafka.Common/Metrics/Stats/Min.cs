using System;
using System.Collections.Generic;

namespace Kafka.Common.Metrics.Stats
{
    /**
     * A {@link SampledStat} that gives the min over its samples.
     */
    public Min : SampledStat
    {
        public Min() : base(double.MaxValue)
        {
        }

        protected override void update(Sample sample, MetricConfig config, double value, long now)
        {
            sample.value = Math.Min(sample.value, value);
        }

        public override double combine(List<Sample> samples, MetricConfig config, long now)
        {
            double min = double.MaxValue;
            long count = 0;
            foreach (Sample sample in samples)
            {
                min = Math.Min(min, sample.value);
                count += sample.eventCount;
            }

            return count == 0 ? double.NaN : min;
        }
    }
}