using Kafka.Common.Metrics.Stats.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Common.Metrics.Stats
{

    /**
     * A compound stat that reports one or more percentiles
     */
    public class Percentiles : SampledStat, ICompoundStat
    {

        public enum BucketSizing
        {
            CONSTANT, LINEAR
        }

        private int buckets;
        private Percentile[] percentiles;
        private IBinScheme binScheme;

        public Percentiles(int sizeInBytes, double max, BucketSizing bucketing, Percentile[] percentiles)
            : this(sizeInBytes, 0.0, max, bucketing, percentiles)
        {
        }

        public Percentiles(int sizeInBytes, double min, double max, BucketSizing bucketing, Percentile[] percentiles)
                : base(0.0)
        {
            this.percentiles = percentiles;
            this.buckets = sizeInBytes / 4;
            if (bucketing == BucketSizing.CONSTANT)
            {
                this.binScheme = new ConstantBinScheme(buckets, min, max);
            }
            else if (bucketing == BucketSizing.LINEAR)
            {
                if (min != 0.0d)
                    throw new System.ArgumentException("Linear bucket sizing requires min to be 0.0.");
                this.binScheme = new LinearBinScheme(buckets, max);
            }
            else
            {
                throw new System.ArgumentException("Unknown bucket type: " + bucketing);
            }
        }

        public List<NamedMeasurable> stats()
        {
            List<NamedMeasurable> ms = new List<NamedMeasurable>(this.percentiles.Length);

            foreach (Percentile percentile in this.percentiles)
            {
                double pct = percentile.percentile;
                //ms.Add(new NamedMeasurable(percentile.name, new IMeasurable()
                //{
                //    public double measure(MetricConfig config, long now)
{
                //        return value(config, now, pct / 100.0);
                //    }
                //}));
            }

            return ms;
        }

        public double value(MetricConfig config, long now, double quantile)
        {
            purgeObsoleteSamples(config, now);
            float count = 0.0f;
            foreach (Sample sample in this.samples)
                count += sample.eventCount;

            if (count == 0.0f)
                return double.NaN;
            float sum = 0.0f;
            float quant = (float)quantile;
            for (int b = 0; b < buckets; b++)
            {
                foreach (Sample s in this.samples)
                {
                    HistogramSample sample = (HistogramSample)s;
                    float[] hist = sample.histogram.counts();
                    sum += hist[b];
                    if (sum / count > quant)
                        return binScheme.fromBin(b);
                }
            }

            return double.PositiveInfinity;
        }

        public override double combine(List<Sample> samples, MetricConfig config, long now)
        {
            return value(config, now, 0.5);
        }

        protected override Sample newSample(long timeMs)
        {
            return new HistogramSample(this.binScheme, timeMs);
        }

        protected override void update(Sample sample, MetricConfig config, double value, long timeMs)
        {
            HistogramSample hist = (HistogramSample)sample;
            hist.histogram.record(value);
        }
    }
}
