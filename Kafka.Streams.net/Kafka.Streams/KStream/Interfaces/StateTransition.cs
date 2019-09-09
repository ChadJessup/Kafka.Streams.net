using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.KStream.Interfaces
{
    public class StateTransition<States>
        where States : Enum
    {
        public States StartingState { get; }
        public HashSet<States> PossibleTransitions { get; }

        public StateTransition(States startingState)
            => this.StartingState = startingState;

        public StateTransition(States startingState, params States[] validTransitions)
            : this(startingState)
            => this.PossibleTransitions = new HashSet<States>(validTransitions);

        public StateTransition(States startingState, params int[] validTransitions)
            : this(startingState, validTransitions.Select(t => (States)Enum.Parse(typeof(States), t.ToString())).ToArray())
        {
        }
    }
}
