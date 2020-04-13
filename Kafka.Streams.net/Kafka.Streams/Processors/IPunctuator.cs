using System;

namespace Kafka.Streams.Processors
{
    /**
     * A functional interface used as an argument to {@link IProcessorContext#schedule(TimeSpan, PunctuationType, Punctuator)}.
     *
     * @see Cancellable
     */
    public interface IPunctuator
    {
        /**
         * Perform the scheduled periodic operation.
         *
         * @param timestamp when the operation is being called, depending on {@link PunctuationType}
         */
        void Punctuate(DateTime timestamp);
    }
}
