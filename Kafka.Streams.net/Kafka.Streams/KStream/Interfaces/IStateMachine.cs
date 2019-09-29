using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Interfaces
{
    public interface IStateMachine<States> : IThreadStateTransitionValidator<States>
        where States : Enum
    {
        IStateListener StateListener { get; }
        States CurrentState { get; }
        bool setState(States state);
        void setTransitions(IEnumerable<StateTransition<States>> validTransitions);
        bool isRunning();
        IThread<States> Thread { get; }
    }
}
