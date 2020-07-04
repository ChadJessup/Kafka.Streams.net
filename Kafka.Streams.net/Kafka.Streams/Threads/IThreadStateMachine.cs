using Kafka.Streams.Processors.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Threads
{
    public interface IStateMachine<States>
        where States : Enum
    {
        States CurrentState { get; }
        bool SetState(States state);
        void SetTransitions(IEnumerable<StateTransition<States>> validTransitions);
        bool IsRunning();
    }

    public interface IThreadStateMachine<States> : IStateMachine<States>, IThreadStateTransitionValidator<States>, IStateObserver, IThreadObserver<States>
        where States : Enum
    {
    }
}
