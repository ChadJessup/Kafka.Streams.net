using Kafka.Streams.Errors;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Threads.GlobalStream
{
    /**
     * This is the thread responsible for keeping all Global State Stores updated.
     * It delegates most of the responsibility to the internal StateConsumer
     */
    public class GlobalStreamThreadState : IStateMachine<GlobalStreamThreadStates>
    {
        private readonly ILogger<GlobalStreamThreadState> logger;
        private Dictionary<GlobalStreamThreadStates, StateTransition<GlobalStreamThreadStates>> validTransitions = new Dictionary<GlobalStreamThreadStates, StateTransition<GlobalStreamThreadStates>>();
        private readonly object stateLock = new object();
        private readonly string logPrefix = "";

        public GlobalStreamThreadState(ILogger<GlobalStreamThreadState> logger)
        {
            this.logger = logger;

            this.SetTransitions(new List<StateTransition<GlobalStreamThreadStates>>
            {
                new StateTransition<GlobalStreamThreadStates>(GlobalStreamThreadStates.CREATED, 1, 2),
                new StateTransition<GlobalStreamThreadStates>(GlobalStreamThreadStates.RUNNING, 2),
                new StateTransition<GlobalStreamThreadStates>(GlobalStreamThreadStates.PENDING_SHUTDOWN, 3),
                new StateTransition<GlobalStreamThreadStates>(GlobalStreamThreadStates.DEAD),
            });

            this.CurrentState = GlobalStreamThreadStates.CREATED;
        }

        public GlobalStreamThreadStates CurrentState { get; private set; } = GlobalStreamThreadStates.CREATED;
        public IStateListener StateListener { get; protected set; }
        public IThread<GlobalStreamThreadStates> Thread { get; protected set; }

        public bool IsRunning()
            => this.CurrentState == GlobalStreamThreadStates.RUNNING;

        public void SetTransitions(IEnumerable<StateTransition<GlobalStreamThreadStates>> validTransitions)
        {
            this.validTransitions =
                validTransitions.ToDictionary(k => k.StartingState, v => v);
        }

        public bool IsValidTransition(GlobalStreamThreadStates newState)
            => this.validTransitions.ContainsKey(newState)
                ? this.validTransitions[this.CurrentState].PossibleTransitions.Contains(newState)
                : false;

        public bool SetState(GlobalStreamThreadStates newState)
        {
            GlobalStreamThreadStates oldState = this.CurrentState;

            lock (stateLock)
            {
                if (this.CurrentState == GlobalStreamThreadStates.PENDING_SHUTDOWN
                    && newState == GlobalStreamThreadStates.PENDING_SHUTDOWN)
                {
                    // when the state is already in PENDING_SHUTDOWN, its transition to itself
                    // will be refused but we do not throw exception here
                    return false;
                }
                else if (this.CurrentState == GlobalStreamThreadStates.DEAD)
                {
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return false;
                }
                else if (!this.IsValidTransition(newState))
                {
                    this.logger.LogError("Unexpected state transition from {} to {}", oldState, newState);
                    throw new StreamsException(logPrefix + "Unexpected state transition from " + oldState + " to " + newState);
                }
                else
                {
                    this.logger.LogInformation("State transition from {} to {}", oldState, newState);
                }

                CurrentState = newState;
            }

            if (this.StateListener != null)
            {
                this.StateListener.OnChange(this.Thread, this.CurrentState, oldState);
            }

            return true;
        }

        public void SetStateListener(IStateListener stateListener)
            => this.StateListener = stateListener;

        public void SetThread(IThread<GlobalStreamThreadStates> thread)
            => this.Thread = thread;
    }
}