using Kafka.Streams.Threads.KafkaStream;
using System;
using System.Collections.Generic;

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

        void SetThreadStates(Dictionary<long, KafkaStreamThreadState> threadStates);
    }
}
