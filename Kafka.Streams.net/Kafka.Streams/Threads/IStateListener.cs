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
        void OnChange(
            IThread<States> thread,
            States newState,
            States oldState);

        new void OnChange(
            IThread thread,
            object newState,
            object oldState)
        { }
    }

    public interface IStateListener
    {
        /**
         * Called when state changes.
         *
         * @param newState new state
         * @param oldState previous state
         */
        void OnChange(
            IThread thread,
            object newState,
            object oldState);

        void SetThreadStates(Dictionary<long, StreamThreadState> threadStates);
    }
}
