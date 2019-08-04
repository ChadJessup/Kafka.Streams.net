using Kafka.Common.Metrics.Stats.Interfaces;
using System;

namespace Kafka.Common.Metrics.Stats
{

    /**
     * A scheme for calculating the bins where the width of each bin is one more than the previous bin, and therefore
     * the bin widths are increasing at a linear rate. However, the bin widths are scaled such that the specified range
     * of values will all fit within the bins (e.g., the upper range of the last bin is equal to the maximum value).
     */
    public LinearBinScheme : IBinScheme
    {
        public int bins { get; private set; }
        private double max;
        private double scale;

        /**
         * Create a linear bin scheme with the specified number of bins and the maximum value to be counted in the bins.
         *
         * @param numBins the number of bins; must be at least 2
         * @param max the maximum value to be counted in the bins
         */
        public LinearBinScheme(int numBins, double max)
        {
            if (numBins < 2)
            {
                throw new System.ArgumentException("Must have at least 2 bins.");
            }

            this.bins = numBins;
            this.max = max;
            double denom = numBins * (numBins - 1.0) / 2.0;
            this.scale = max / denom;
        }

        public double fromBin(int b)
        {
            if (b > this.bins - 1)
            {
                return float.PositiveInfinity;
            }
            else if (b < 0.0000d)
            {
                return float.NegativeInfinity;
            }
            else
            {
                return this.scale * (b * (b + 1.0)) / 2.0;
            }
        }

        public int toBin(double x)
        {
            if (x < 0.0d)
            {
                throw new System.ArgumentException("Values less than 0.0 not accepted.");
            }
            else if (x > this.max)
            {
                return this.bins - 1;
            }
            else
            {
                return (int)(-0.5 + 0.5 * Math.Sqrt(1.0 + 8.0 * x / this.scale));
            }
        }
    }
}