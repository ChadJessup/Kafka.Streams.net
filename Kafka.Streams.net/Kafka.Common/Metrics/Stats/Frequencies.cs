using Kafka.Common.Metrics.Stats.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using static Kafka.Common.Metrics.Stats.Histogram;

namespace Kafka.Common.Metrics.Stats
{

    /**
     * A {@link CompoundStat} that represents a normalized distribution with a {@link Frequency} metric for each
     * bucketed value. The values of the {@link Frequency} metrics specify the frequency of the center value appearing
     * relative to the total number of values recorded.
     * <p>
     * For example, consider a component that records failure or success of an operation using bool values, with
     * one metric to capture the percentage of operations that failed another to capture the percentage of operations
     * that succeeded.
     * <p>
     * This can be accomplish by created a {@link org.apache.kafka.common.metrics.Sensor Sensor} to record the values,
     * with 0.0 for false and 1.0 for true. Then, create a single {@link Frequencies} object that has two
     * {@link Frequency} metrics: one centered around 0.0 and another centered around 1.0. The {@link Frequencies}
     * object is a {@link CompoundStat}, and so it can be {@link org.apache.kafka.common.metrics.Sensor.Add(CompoundStat)
     *.Added directly to a Sensor} so the metrics are created automatically.
     */
    public class Frequencies : SampledStat, ICompoundStat
    {

        /**
         * Create a Frequencies instance with metrics for the frequency of a bool sensor that records 0.0 for
         * false and 1.0 for true.
         *
         * @param falseMetricName the name of the metric capturing the frequency of failures; may be null if not needed
         * @param trueMetricName  the name of the metric capturing the frequency of successes; may be null if not needed
         * @return the Frequencies instance; never null
         * @throws ArgumentException if both {@code falseMetricName} and {@code trueMetricName} are null
         */
        public static Frequencies forBooleanValues(MetricName falseMetricName, MetricName trueMetricName)
        {
            List<Frequency> frequencies = new List<Frequency>();

            if (falseMetricName != null)
            {
                frequencies.Add(new Frequency(falseMetricName, 0.0));
            }

            if (trueMetricName != null)
            {
                frequencies.Add(new Frequency(trueMetricName, 1.0));
            }

            if (!frequencies.Any())
            {
                throw new System.ArgumentException("Must specify at least one metric name");
            }

            return new Frequencies(2, 0.0, 1.0, frequencies);
        }

        private List<Frequency> frequencies;
        private IBinScheme binScheme;

        /**
         * Create a Frequencies that captures the values in the specified range into the given number of buckets,
         * where the buckets are centered around the minimum, maximum, and intermediate values.
         *
         * @param buckets     the number of buckets; must be at least 1
         * @param min         the minimum value to be captured
         * @param max         the maximum value to be captured
         * @param frequencies the list of {@link Frequency} metrics, which at most should be one per bucket centered
         *                    on the bucket's value, though not every bucket need to correspond to a metric if the
         *                    value is not needed
         * @throws ArgumentException if any of the {@link Frequency} objects do not have a
         *                                  {@link Frequency#centerValue() center value} within the specified range
         */
        public Frequencies(int buckets, double min, double max, List<Frequency> frequencies)
                : base(0.0)
        {
            if (max < min)
            {
                throw new System.ArgumentException("The maximum value " + max
                                                           + " must be greater than the minimum value " + min);
            }

            if (buckets < 1)
            {
                throw new System.ArgumentException("Must be at least 1 bucket");
            }

            if (buckets < frequencies.Count)
            {
                throw new System.ArgumentException("More frequencies than buckets");
            }
            this.frequencies = frequencies;
            foreach (Frequency freq in frequencies)
            {
                if (min > freq.centerValue || max < freq.centerValue)
                {
                    throw new System.ArgumentException("The frequency centered at '" + freq.centerValue
                                                               + "' is not within the range [" + min + "," + max + "]");
                }
            }
            double halfBucketWidth = (max - min) / (buckets - 1) / 2.0;
            this.binScheme = new ConstantBinScheme(buckets, min - halfBucketWidth, max + halfBucketWidth);
        }

        public List<NamedMeasurable> stats()
        {
            List<NamedMeasurable> ms = new List<NamedMeasurable>(frequencies.Count);
            foreach (Frequency freq in frequencies)
            {
                double center = freq.centerValue;
                //ms.Add(new NamedMeasurable(frequency.name(), new IMeasurable()
                //{
                //    public double measure(MetricConfig config, long now)
                //{
                //        return frequency(config, now, center);
                //    }
                //}));
            }

            return ms;
        }

        /**
         * Return the computed frequency describing the number of occurrences of the values in the bucket for the given
         * center point, relative to the total number of occurrences in the samples.
         *
         * @param config      the metric configuration
         * @param now         the current time in milliseconds
         * @param centerValue the value corresponding to the center point of the bucket
         * @return the frequency of the values in the bucket relative to the total number of samples
         */
        public double frequency(MetricConfig config, long now, double centerValue)
        {
            purgeObsoleteSamples(config, now);
            long totalCount = 0;
            foreach (Sample sample in samples)
            {
                totalCount += sample.eventCount;
            }
            if (totalCount == 0)
            {
                return 0.0d;
            }

            // Add up all of the counts in the bin corresponding to the center value
            float count = 0.0f;
            int binNum = binScheme.toBin(centerValue);
            foreach (Sample s in samples)
            {
                HistogramSample sample = (HistogramSample)s;
                float[] hist = sample.histogram.counts();
                count += hist[binNum];
            }
            // Compute the ratio of counts to total counts
            return count / (double)totalCount;
        }

        double totalCount()
        {
            long count = 0;
            foreach (Sample sample in samples)
            {
                count += sample.eventCount;
            }
            return count;
        }

        public override double combine(List<Sample> samples, MetricConfig config, long now)
        {
            return totalCount();
        }

        protected override Sample newSample(long timeMs)
        {
            return new HistogramSample(binScheme, timeMs);
        }

        protected override void update(Sample sample, MetricConfig config, double value, long timeMs)
        {
            HistogramSample hist = (HistogramSample)sample;
            hist.histogram.record(value);
        }
    }
}