using System;
using System.Collections.Generic;
using Kafka.Streams.Threads.Stream;

namespace Kafka.Streams.Threads.KafkaStreams
{
    internal class WrappedStateListener<States> : IStateListener<States>
        where States : Enum
    {
        private Action<IThread<States>, States, States>? onChangeWithThread;
        private Action<States, States>? onChange;

        public WrappedStateListener(Action<IThread<States>, States, States> onChange)
            => this.onChangeWithThread = onChange;

        public WrappedStateListener(Action<States, States> onChange)
            => this.onChange = onChange;

        void IStateListener.OnChange<States1>(
            IThread<States1> thread,
            States1 newState,
            States1 oldState)
            where States1 : States
        {
            this.OnChange(thread, newState, oldState);
        }

        public void OnChange(IThread<States> thread, States newState, States oldState)
        {
            if (this.onChangeWithThread != null)
            {
                this.onChangeWithThread?.Invoke(thread, newState, oldState);
            }
            else
            {
                this.onChange?.Invoke(newState, oldState);
            }
        }

        public void SetThreadStates(Dictionary<long, StreamThreadState> threadStates)
        {
        }
    }
}
