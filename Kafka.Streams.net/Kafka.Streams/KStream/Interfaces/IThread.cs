using Kafka.Streams.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Kafka.Streams.KStream.Interfaces
{
    public interface IThread
    {
        Thread Thread { get; }
        bool stillRunning();
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
