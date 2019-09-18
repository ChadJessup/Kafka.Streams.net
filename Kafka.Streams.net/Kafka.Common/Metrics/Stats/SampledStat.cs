using System.Collections.Generic;

namespace Kafka.Common.Metrics.Stats
{
    /**
     * A SampledStat records a single scalar value measured over one or more samples. Each sample is recorded over a
     * configurable window. The window can be defined by number of events or elapsed time (or both, if both are given the
     * window is complete when <i>either</i> the event count or elapsed time criterion is met).
     * <p>
     * All the samples are combined to produce the measurement. When a window is complete the oldest sample is cleared and
     * recycled to begin recording the next sample.
     *
     * Sues of this define different statistics measured using this basic pattern.
     */
    public abstract class SampledStat : IMeasurableStat
    {
        private readonly double initialValue;
        private int current = 0;
        protected List<Sample> samples;

        public SampledStat(double initialValue)
        {
            this.initialValue = initialValue;
            this.samples = new List<Sample>(2);
        }

        public void record(MetricConfig config, double value, long timeMs)
        {
            Sample sample = Current(timeMs);
            if (sample.isComplete(timeMs, config))
                sample = advance(config, timeMs);
            update(sample, config, value, timeMs);
            sample.eventCount += 1;
        }

        private Sample advance(MetricConfig config, long timeMs)
        {
            this.current = (this.current + 1) % config.samples;
            if (this.current >= samples.Count)
            {
                Sample sample = newSample(timeMs);
                this.samples.Add(sample);
                return sample;
            }
            else
            {
                Sample sample = Current(timeMs);
                sample.reset(timeMs);
                return sample;
            }
        }

        protected virtual Sample newSample(long timeMs)
        {
            return new Sample(this.initialValue, timeMs);
        }

        public double measure(MetricConfig config, long now)
        {
            purgeObsoleteSamples(config, now);
            return combine(this.samples, config, now);
        }

        public Sample Current(long timeMs)
        {
            if (samples.Count == 0)
                this.samples.Add(newSample(timeMs));
            return this.samples[this.current];
        }

        public Sample oldest(long now)
        {
            if (samples.Count == 0)
                this.samples.Add(newSample(now));

            Sample oldest = this.samples[0];
            for (int i = 1; i < this.samples.Count; i++)
            {
                Sample curr = this.samples[i];
                if (curr.lastWindowMs < oldest.lastWindowMs)
                    oldest = curr;
            }
            return oldest;
        }

        protected abstract void update(Sample sample, MetricConfig config, double value, long timeMs);

        public abstract double combine(List<Sample> samples, MetricConfig config, long now);

        /* Timeout any windows that have expired in the absence of any events */
        public void purgeObsoleteSamples(MetricConfig config, long now)
        {
            long expireAge = config.samples * config.timeWindowMs;
            foreach (Sample sample in samples)
            {
                if (now - sample.lastWindowMs >= expireAge)
                    sample.reset(now);
            }
        }
    }
}