namespace Kafka.Streams.Processors
{
    /**
     * A functional interface used as an argument to {@link IProcessorContext#schedule(Duration, PunctuationType, Punctuator)}.
     *
     * @see Cancellable
     */
    public interface Punctuator
    {
        /**
         * Perform the scheduled periodic operation.
         *
         * @param timestamp when the operation is being called, depending on {@link PunctuationType}
         */
        void punctuate(long timestamp);
    }
}