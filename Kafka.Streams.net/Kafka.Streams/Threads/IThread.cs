using System;

namespace Kafka.Streams.Threads
{
    public interface IThread
    {
        int ManagedThreadId { get; }
        void Start();
        bool IsRunning();
    }

    // We can't derive from the Thread object, so we'll wrap threads
    // with classes that implement this interface.
    public interface IThread<States> : IThread
        where States : Enum
    {
        string ThreadClientId { get; }
        IThreadContext<IThread<States>, IStateMachine<States>, States> Context { get; }
        void SetContext(IThreadContext<IThread<States>, IStateMachine<States>, States> context);
    }
}
