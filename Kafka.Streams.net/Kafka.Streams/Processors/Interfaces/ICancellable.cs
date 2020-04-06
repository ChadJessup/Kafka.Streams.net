namespace Kafka.Streams.Processors.Interfaces
{
    /**
     * Cancellable interface returned in {@link IProcessorContext#schedule(TimeSpan, PunctuationType, Punctuator)}.
     *
     * @see Punctuator
     */
    public interface ICancellable
    {
        /**
         * Cancel the scheduled operation to avoid future calls.
         */
        void Cancel();
    }
}