using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Threads
{
    public interface IStateMachine<States> : IThreadStateTransitionValidator<States>
        where States : Enum
    {
        IStateListener StateListener { get; }
        States CurrentState { get; }
        bool SetState(States state);
        void SetTransitions(IEnumerable<StateTransition<States>> validTransitions);
        bool IsRunning();
        IThread<States> Thread { get; }
    }
}
