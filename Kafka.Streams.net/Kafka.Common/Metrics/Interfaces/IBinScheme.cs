namespace Kafka.Common.Metrics.Stats.Interfaces
{
    /**
     * An algorithm for determining the bin in which a value is to be placed as well as calculating the upper end
     * of each bin.
     */
    public interface IBinScheme
    {

        /**
         * Get the number of bins.
         *
         * @return the number of bins
         */
        int bins { get; }

        /**
         * Determine the 0-based bin number in which the supplied value should be placed.
         *
         * @param value the value
         * @return the 0-based index of the bin
         */
        int toBin(double value);

        /**
         * Determine the value at the upper range of the specified bin.
         *
         * @param bin the 0-based bin number
         * @return the value at the upper end of the bin; or {@link Float#NegativeInfinity negative infinity}
         * if the bin number is negative or {@link Float#PositiveInfinity positive infinity} if the 0-based
         * bin number is greater than or equal to the {@link #bins() number of bins}.
         */
        double fromBin(int bin);
    }
}