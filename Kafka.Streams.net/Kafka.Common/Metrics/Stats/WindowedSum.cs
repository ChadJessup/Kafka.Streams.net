using System.Collections.Generic;

namespace Kafka.Common.Metrics.Stats
{
    /**
     * A {@link SampledStat} that maintains the sum of what it has seen.
     * This is a sampled version of {@link CumulativeSum}.
     *
     * See also {@link WindowedCount} if you want to increment the value by 1 on each recording.
     */
    public class WindowedSum : SampledStat
    {
        public WindowedSum()
            : base(0)
        {
        }

        protected override void update(Sample sample, MetricConfig config, double value, long now)
{
            sample.value += value;
        }

        public override double combine(List<Sample> samples, MetricConfig config, long now)
{
            double total = 0.0;
            foreach (Sample sample in samples)
                total += sample.value;
            return total;
        }

    }
}