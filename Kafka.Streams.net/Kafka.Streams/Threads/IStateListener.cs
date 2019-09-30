using System;

namespace Kafka.Streams.Threads
{
    /**
     * Listen to {@link State} change events.
     */
    public interface IStateListener
    {
        /**
         * Called when state changes.
         *
         * @param newState new state
         * @param oldState previous state
         */
        void onChange<States>(
            IThread<States> thread, 
            States newState,
            States oldState)
            where States : Enum;
    }
}
