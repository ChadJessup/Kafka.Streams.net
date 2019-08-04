using System.Collections.Generic;

namespace Kafka.Common.Metrics.Stats
{
    /**
     * A {@link SampledStat} that maintains a simple average over its samples.
     */
    public class Avg : SampledStat
    {
        public Avg()
          : base(0.0)
        {
        }

        protected override void update(Sample sample, MetricConfig config, double value, long now)
        {
            sample.value += value;
        }

        public override double combine(List<Sample> samples, MetricConfig config, long now)
        {
            double total = 0.0;
            long count = 0;
            foreach (Sample s in samples)
            {
                total += s.value;
                count += s.eventCount;
            }
            return count == 0 ? double.NaN : total / count;
        }
    }
}

