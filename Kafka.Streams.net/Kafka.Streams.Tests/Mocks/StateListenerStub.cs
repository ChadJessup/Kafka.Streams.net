using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Threads;
using Kafka.Streams.Threads.Stream;
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

        public void OnChange(
            IThread thread,
            object newState,
            object oldState)
        {
            var prevCount = this.mapStates.ContainsKey(newState)
                ? this.mapStates[newState]
                : 0;

            this.NumChanges++;
            this.OldState = oldState;
            this.NewState = newState;
            this.mapStates[newState] = prevCount + 1;
        }

        public void OnChange<States>(
            IThread<States> thread,
            States newState,
            States oldState)
            where States : Enum
            => this.OnChange(thread,
                newState,
                oldState);

        public void SetThreadStates(Dictionary<long, StreamThreadState> threadStates)
        {
        }
    }
}
