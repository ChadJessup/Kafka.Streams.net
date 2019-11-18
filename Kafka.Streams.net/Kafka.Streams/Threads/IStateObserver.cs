namespace Kafka.Streams.Threads
{
    public interface IStateObserver
    {
        IStateListener StateListener { get; }
        void SetStateListener(IStateListener stateListener);
    }
}
