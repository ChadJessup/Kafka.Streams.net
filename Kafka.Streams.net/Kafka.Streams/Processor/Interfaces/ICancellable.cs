namespace Kafka.Streams.IProcessor.Interfaces
{
    /**
     * Cancellable interface returned in {@link IProcessorContext#schedule(Duration, PunctuationType, Punctuator)}.
     *
     * @see Punctuator
     */
    public interface ICancellable
    {
        /**
         * Cancel the scheduled operation to avoid future calls.
         */
        void cancel();
    }
}