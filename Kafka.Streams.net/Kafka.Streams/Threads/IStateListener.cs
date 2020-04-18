using Kafka.Streams.Threads.Stream;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Threads
{
    /**
     * Listen to {@link State} change events.
     */
    public interface IStateListener<States> : IStateListener
       where States : Enum
    {
        void OnChange(IThread<States> thread, States newState, States oldState);
        new void OnChange<States1>(
            IThread<States1> thread,
            States1 newState,
            States1 oldState)
            where States1 : States;
    }

    public interface IStateListener
    {
        /**
         * Called when state changes.
         *
         * @param newState new state
         * @param oldState previous state
         */
        void OnChange<States>(
            IThread<States> thread,
            States newState,
            States oldState)
            where States : Enum;

        void SetThreadStates(Dictionary<long, StreamThreadState> threadStates);
    }
}
