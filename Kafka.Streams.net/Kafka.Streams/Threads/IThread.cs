using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using System;
using System.Threading;

namespace Kafka.Streams.Threads
{
    public interface IThread
    {
        Thread Thread { get; }
        bool isRunning();
    }

    // We can't derive from the Thread object, so we'll wrap threads
    // with classes that implement this interface.
    public interface IThread<States> : IThread
        where States : Enum
    {
        string ThreadClientId { get; }
        IStateListener StateListener { get; }
        IStateMachine<States> State { get; }
        void setStateListener(IStateListener listener);
    }
}
