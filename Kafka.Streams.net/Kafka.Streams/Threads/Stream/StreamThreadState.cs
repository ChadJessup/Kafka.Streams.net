using Kafka.Streams.Errors;
using Kafka.Streams.Tasks;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Threads.Stream
{
    public class StreamThreadState : IStateMachine<StreamThreadStates>
    {
        private readonly object stateLock = new object();
        private readonly ILogger<StreamThreadState> logger;
        private Dictionary<StreamThreadStates, StateTransition<StreamThreadStates>> validTransitions = new Dictionary<StreamThreadStates, StateTransition<StreamThreadStates>>();

        public StreamThreadState(ILogger<StreamThreadState> logger)
        {
            this.logger = logger;

            this.SetTransitions(new List<StateTransition<StreamThreadStates>>
            {
                new StateTransition<StreamThreadStates>(StreamThreadStates.CREATED, StreamThreadStates.STARTING, StreamThreadStates.PENDING_SHUTDOWN),
                new StateTransition<StreamThreadStates>(StreamThreadStates.STARTING, StreamThreadStates.PARTITIONS_REVOKED, StreamThreadStates.PENDING_SHUTDOWN),
                new StateTransition<StreamThreadStates>(StreamThreadStates.PARTITIONS_REVOKED, StreamThreadStates.PARTITIONS_ASSIGNED, StreamThreadStates.PENDING_SHUTDOWN),
                new StateTransition<StreamThreadStates>(StreamThreadStates.PARTITIONS_ASSIGNED, StreamThreadStates.PARTITIONS_REVOKED, StreamThreadStates.RUNNING, StreamThreadStates.PENDING_SHUTDOWN),
                new StateTransition<StreamThreadStates>(StreamThreadStates.RUNNING, StreamThreadStates.PARTITIONS_REVOKED, StreamThreadStates.PENDING_SHUTDOWN),
                new StateTransition<StreamThreadStates>(StreamThreadStates.PENDING_SHUTDOWN, StreamThreadStates.DEAD),
                new StateTransition<StreamThreadStates>(StreamThreadStates.DEAD),
            });

            this.CurrentState = StreamThreadStates.CREATED;
        }

        private volatile StreamThreadStates currentState;
        public StreamThreadStates CurrentState
        {
            get
            {
                lock (stateLock)
                {
                    return this.currentState;
                }
            }

            protected set
            {
                lock (stateLock)
                {
                    this.currentState = value;
                }
            }
        }

        public IStateListener StateListener { get; protected set; }
        public ITaskManager TaskManager { get; private set; }
        public IThread<StreamThreadStates> Thread { get; protected set; }

        public bool isValidTransition(StreamThreadStates newState)
            => this.validTransitions.ContainsKey(newState)
                ? this.validTransitions[this.CurrentState].PossibleTransitions.Contains(newState)
                : false;

        public void SetTaskManager(ITaskManager taskManager)
            => this.TaskManager = taskManager;

        /**
         * Sets the state
         *
         * @param newState New state
         * @return The state prior to the call to setState, or null if the transition is invalid
         */
        public bool SetState(StreamThreadStates newState)
        {
            StreamThreadStates oldState;

            lock (stateLock)
            {
                oldState = this.CurrentState;

                if (this.CurrentState == StreamThreadStates.PENDING_SHUTDOWN
                    && newState != StreamThreadStates.DEAD)
                {
                    logger.LogDebug($"Ignoring request to transit from PENDING_SHUTDOWN to {newState}: only DEAD state is a valid next state");

                    // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                    // refused but we do not throw exception here
                    return false;
                }
                else if (this.CurrentState == StreamThreadStates.DEAD)
                {
                    logger.LogDebug($"Ignoring request to transit from DEAD to {newState}: no valid next state after DEAD");

                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return false;
                }
                else if (this.CurrentState == StreamThreadStates.PARTITIONS_REVOKED
                    && newState == StreamThreadStates.PARTITIONS_REVOKED)
                {
                    this.logger.LogDebug("Ignoring request to transit from PARTITIONS_REVOKED to PARTITIONS_REVOKED: " +
                                  "self transition is not allowed");
                    // when the state is already in PARTITIONS_REVOKED, its transition to itself will be
                    // refused but we do not throw exception here
                    return false;
                }
                else if (!this.isValidTransition(newState))
                {
                    this.logger.LogError($"Unexpected state transition from {oldState} to {newState}");

                    throw new StreamsException($"Unexpected state transition from {oldState} to {newState}");
                }
                else
                {
                    this.logger.LogInformation($"StreamThreadState transition from {oldState} to {newState}");
                }

                this.currentState = newState;

                if (newState == StreamThreadStates.RUNNING)
                {
                    if (this.Thread is StreamThread st)
                    {
                        st.UpdateThreadMetadata(
                            this.TaskManager.activeTasks(),
                            this.TaskManager.StandbyTasks());
                    }
                }
                else
                {
                    if (this.Thread is StreamThread st)
                    {
                        st.UpdateThreadMetadata(
                            activeTasks: new Dictionary<TaskId, StreamTask>(),
                            standbyTasks: new Dictionary<TaskId, StandbyTask>());
                    }
                }
            }

            this.StateListener?.OnChange(this.Thread, this.CurrentState, oldState);

            return true;
        }

        public void SetTransitions(IEnumerable<StateTransition<StreamThreadStates>> validTransitions)
        {
            this.validTransitions = validTransitions
                .ToDictionary(k => k.StartingState, v => v);
        }

        public bool IsRunning()
            => this.CurrentState == StreamThreadStates.RUNNING
            || this.CurrentState == StreamThreadStates.STARTING
            || this.CurrentState == StreamThreadStates.PARTITIONS_REVOKED
            || this.CurrentState == StreamThreadStates.PARTITIONS_ASSIGNED;

        public void SetStateListener(IStateListener stateListener)
            => this.StateListener = stateListener;

        public void SetThread(IThread<StreamThreadStates> thread)
            => this.Thread = thread;
    }
}
