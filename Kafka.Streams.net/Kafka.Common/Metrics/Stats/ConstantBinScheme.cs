using Kafka.Common.Metrics.Stats.Interfaces;
using System;

namespace Kafka.Common.Metrics.Stats
{
    /**
     * A scheme for calculating the bins where the width of each bin is a constant determined by the range of values
     * and the number of bins.
     */
    public ConstantBinScheme : IBinScheme
    {
        public int bins { get; private set; }

        private static int MIN_BIN_NUMBER = 0;
        private double min;
        private double max;
        private double bucketWidth;
        private int maxBinNumber;

        /**
         * Create a bin scheme with the specified number of bins that all have the same width.
         *
         * @param bins the number of bins; must be at least 2
         * @param min the minimum value to be counted in the bins
         * @param max the maximum value to be counted in the bins
         */
        public ConstantBinScheme(int bins, double min, double max)
        {
            if (bins < 2)
                throw new System.ArgumentException("Must have at least 2 bins.");
            this.min = min;
            this.max = max;
            this.bins = bins;
            this.bucketWidth = (max - min) / bins;
            this.maxBinNumber = bins - 1;
        }

        public double fromBin(int b)
        {
            if (b < MIN_BIN_NUMBER)
            {
                return float.NegativeInfinity;
            }

            if (b > maxBinNumber)
            {
                return float.PositiveInfinity;
            }

            return min + b * bucketWidth;
        }

        public int toBin(double x)
        {
            int binNumber = (int)((x - min) / bucketWidth);
            if (binNumber < MIN_BIN_NUMBER)
            {
                return MIN_BIN_NUMBER;
            }
            if (binNumber > maxBinNumber)
            {
                return maxBinNumber;
            }
            return binNumber;
        }
    }
}