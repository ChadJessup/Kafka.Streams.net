using System;
using System.Collections.Generic;

namespace Kafka.Common.Metrics.Stats
{
    /**
     * A {@link SampledStat} that gives the max over its samples.
     */
    public class Max : SampledStat
    {
        public Max()
            : base(double.NegativeInfinity)
        {
        }


        protected override void update(Sample sample, MetricConfig config, double value, long now)
        {
            sample.value = Math.Max(sample.value, value);
        }


        public override double combine(List<Sample> samples, MetricConfig config, long now)
        {
            double max = double.NegativeInfinity;
            long count = 0;
            foreach (Sample sample in samples)
            {
                max = Math.Max(max, sample.value);
                count += sample.eventCount;
            }

            return count == 0 ? Double.NaN : max;
        }
    }
}
