using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Threads.KafkaStreams
{
    public class KafkaStreamsThreadState : IStateMachine<KafkaStreamsThreadStates>
    {
        private readonly ILogger<KafkaStreamsThreadState> logger;
        private Dictionary<KafkaStreamsThreadStates, StateTransition<KafkaStreamsThreadStates>> validTransitions = new Dictionary<KafkaStreamsThreadStates, StateTransition<KafkaStreamsThreadStates>>();
        private readonly object stateLock = new object();

        public KafkaStreamsThreadState(ILogger<KafkaStreamsThreadState> logger)
        {
            this.logger = logger;

            this.SetTransitions(new List<StateTransition<KafkaStreamsThreadStates>>
            {
                new StateTransition<KafkaStreamsThreadStates>(KafkaStreamsThreadStates.CREATED, KafkaStreamsThreadStates.REBALANCING, KafkaStreamsThreadStates.PENDING_SHUTDOWN),
                new StateTransition<KafkaStreamsThreadStates>(KafkaStreamsThreadStates.REBALANCING, KafkaStreamsThreadStates.RUNNING, KafkaStreamsThreadStates.PENDING_SHUTDOWN, KafkaStreamsThreadStates.ERROR),
                new StateTransition<KafkaStreamsThreadStates>(KafkaStreamsThreadStates.RUNNING, KafkaStreamsThreadStates.REBALANCING, KafkaStreamsThreadStates.PENDING_SHUTDOWN, KafkaStreamsThreadStates.ERROR),
                new StateTransition<KafkaStreamsThreadStates>(KafkaStreamsThreadStates.PENDING_SHUTDOWN, KafkaStreamsThreadStates.NOT_RUNNING),
                new StateTransition<KafkaStreamsThreadStates>(KafkaStreamsThreadStates.NOT_RUNNING),
                new StateTransition<KafkaStreamsThreadStates>(KafkaStreamsThreadStates.ERROR, KafkaStreamsThreadStates.PENDING_SHUTDOWN),
                new StateTransition<KafkaStreamsThreadStates>(KafkaStreamsThreadStates.DEAD),
            });

            this.CurrentState = KafkaStreamsThreadStates.CREATED;
        }

        private KafkaStreamsThreadStates currentState = KafkaStreamsThreadStates.UNKNOWN;
        public KafkaStreamsThreadStates CurrentState
        {
            get
            {
                lock (stateLock)
                {
                    return this.currentState;
                }
            }

            private set
            {
                lock (stateLock)
                {
                    this.currentState = value;
                }
            }
        }

        public IStateListener StateListener { get; private set; }
        public IThread<KafkaStreamsThreadStates> Thread { get; private set; }

        public bool IsRunning()
        {
            return this.CurrentState == KafkaStreamsThreadStates.RUNNING
                || this.CurrentState == KafkaStreamsThreadStates.REBALANCING;
        }

        public bool isValidTransition(KafkaStreamsThreadStates newState)
            => this.validTransitions.ContainsKey(newState)
                ? this.validTransitions[this.CurrentState].PossibleTransitions.Contains(newState)
                : false;

        public bool SetState(KafkaStreamsThreadStates newState)
        {
            KafkaStreamsThreadStates oldState;

            lock (stateLock)
            {
                oldState = this.CurrentState;

                if (this.CurrentState == KafkaStreamsThreadStates.PENDING_SHUTDOWN
                    && newState != KafkaStreamsThreadStates.NOT_RUNNING)
                {
                    // when the state is already in PENDING_SHUTDOWN, all other transitions than NOT_RUNNING (due to thread dying) will be
                    // refused but we do not throw exception here, to allow appropriate error handling
                    return false;
                }
                else if (this.CurrentState == KafkaStreamsThreadStates.NOT_RUNNING
                    && (newState == KafkaStreamsThreadStates.PENDING_SHUTDOWN
                        || newState == KafkaStreamsThreadStates.NOT_RUNNING))
                {
                    // when the state is already in NOT_RUNNING, its transition to PENDING_SHUTDOWN or NOT_RUNNING (due to consecutive close calls)
                    // will be refused but we do not throw exception here, to allow idempotent close calls
                    return false;
                }
                else if (this.CurrentState == KafkaStreamsThreadStates.REBALANCING
                    && newState == KafkaStreamsThreadStates.REBALANCING)
                {
                    // when the state is already in REBALANCING, it should not transit to REBALANCING again
                    return false;
                }
                else if (this.CurrentState == KafkaStreamsThreadStates.ERROR
                    && newState == KafkaStreamsThreadStates.ERROR)
                {
                    // when the state is already in ERROR, it should not transit to ERROR again
                    return false;
                }
                else if (!this.isValidTransition(newState))
                {
                    // throw new IllegalStateException("Stream-client " + clientId + ": Unexpected state transition from " + oldState + " to " + newState);
                }
                else
                {
                    this.logger.LogInformation("State transition from {} to {}", oldState, newState);
                }

                this.CurrentState = newState;
                // stateLock.notifyAll();
            }

            // we need to call the user customized state listener outside the state lock to avoid potential deadlocks
            if (this.StateListener != null)
            {
                this.StateListener.OnChange(this.Thread, newState, oldState);
            }

            this.CurrentState = newState;

            return true;
        }

        public void SetTransitions(IEnumerable<StateTransition<KafkaStreamsThreadStates>> validTransitions)
        {
            this.validTransitions = validTransitions
                .ToDictionary(k => k.StartingState, v => v);
        }

        public void SetStateListener(IStateListener stateListener)
            => this.StateListener = stateListener;

        public void SetThread(IThread<KafkaStreamsThreadStates> thread)
            => this.Thread = thread;
    }
}
