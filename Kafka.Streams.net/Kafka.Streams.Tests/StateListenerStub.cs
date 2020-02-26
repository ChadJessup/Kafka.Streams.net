using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Threads;
using Kafka.Streams.Threads.KafkaStream;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Kafka.Streams.Tests
{
    public class StateListenerStub : IStateListener
    {
        public int NumChanges { get; private set; } = 0;
        public object? OldState { get; private set; } = null;
        public object? NewState { get; private set; } = null;
        public ConcurrentDictionary<object, long> mapStates = new ConcurrentDictionary<object, long>();
        public void onChange<States>(
            IThread<States> thread,
            States newState,
            States oldState)
            where States : Enum
        {
            long prevCount = mapStates.ContainsKey(newState)
                ? mapStates[newState]
                : 0;

            this.NumChanges++;
            this.OldState = oldState;
            this.NewState = newState;
            mapStates[newState] = prevCount + 1;
        }

        public void SetThreadStates(Dictionary<long, KafkaStreamThreadState> threadStates)
        {
            throw new NotImplementedException();
        }
    }
}
