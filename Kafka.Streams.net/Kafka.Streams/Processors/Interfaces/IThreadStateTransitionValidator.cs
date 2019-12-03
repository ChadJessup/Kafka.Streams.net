namespace Kafka.Streams.Processors.Interfaces
{
    /**
     * Basic interface for keeping track of the state of a thread.
     */
    public interface IThreadStateTransitionValidator<States>
    {
        bool isValidTransition(States newState);
    }
}