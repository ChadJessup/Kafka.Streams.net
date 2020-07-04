using System;

namespace Kafka.Streams.Threads
{
    public interface IThread : IStateObserver
    {
        int ManagedThreadId { get; }
        void Start();
        void Join();
        bool IsRunning();
    }

    // We can't derive from the Thread object, so we'll wrap threads
    // with classes that implement this interface.
    public interface IThread<States> : IThread
        where States : Enum
    {
        string ThreadClientId { get; }
        IThreadStateMachine<States> State { get; }
    }
}
