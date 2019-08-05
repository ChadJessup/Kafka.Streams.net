using System.Threading;

namespace Kafka.Streams.Processor.Internals
{
    /**
     * Listen to state change events
     */
    public interface StateListener
    {


        /**
         * Called when state changes
         *
         * @param thread   thread changing state
         * @param newState current state
         * @param oldState previous state
         */
        void onChange(Thread thread, ThreadStateTransitionValidator newState, ThreadStateTransitionValidator oldState);
    }
}
