using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor.Internals;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Processors.Internals
{
    public class StreamThreadState : IStateMachine<StreamThreadStates>
    {
        private readonly object stateLock = new object();
        private readonly ILogger<StreamThreadState> logger;
        private Dictionary<StreamThreadStates, StateTransition<StreamThreadStates>> validTransitions = new Dictionary<StreamThreadStates, StateTransition<StreamThreadStates>>();
        private readonly string logPrefix = "";

        public StreamThreadState(
            ILogger<StreamThreadState> logger,
            StreamStateListener stateListener,
            TaskManager taskManager)
        {
            this.logger = logger;
            this.StateListener = stateListener;
            this.TaskManager = taskManager;
        }

        public StreamThreadStates CurrentState { get; protected set; }
        public IStateListener StateListener { get; }
        public TaskManager TaskManager { get; }
        public IThread<StreamThreadStates> Thread { get; }

        public bool isValidTransition(StreamThreadStates newState)
            => this.validTransitions.ContainsKey(newState)
                ? this.validTransitions[newState].PossibleTransitions.Contains(newState)
                : false;
        /**
         * Sets the state
         *
         * @param newState New state
         * @return The state prior to the call to setState, or null if the transition is invalid
         */
        public bool setState(StreamThreadStates newState)
        {
            StreamThreadStates oldState;

            lock (stateLock)
            {
                oldState = this.CurrentState;

                if (this.CurrentState == StreamThreadStates.PENDING_SHUTDOWN
                    && newState != StreamThreadStates.DEAD)
                {
                    logger.LogDebug("Ignoring request to transit from PENDING_SHUTDOWN to {}: " +
                                  "only DEAD state is a valid next state", newState);
                    // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                    // refused but we do not throw exception here
                    return false;
                }
                else if (this.CurrentState == StreamThreadStates.DEAD)
                {
                    logger.LogDebug("Ignoring request to transit from DEAD to {}: " +
                                  "no valid next state after DEAD", newState);
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

                    throw new StreamsException(this.logPrefix + "Unexpected state transition from " + oldState + " to " + newState);
                }
                else
                {
                    logger.LogInformation("StreamThreadState transition from {} to {}", oldState, newState);
                }

                this.CurrentState = newState;
                if (newState == StreamThreadStates.RUNNING)
                {
                    if (this.Thread is StreamThread st)
                    {
                        st.updateThreadMetadata(TaskManager.activeTasks(), TaskManager.standbyTasks());
                    }
                }
                else
                {
                    if (this.Thread is StreamThread st)
                    {
                        st.updateThreadMetadata(null, null);
                    }
                }
            }

            if (StateListener != null)
            {
                StateListener.onChange(this.Thread, this.CurrentState, oldState);
            }

            return true;
        }

        public void setTransitions(IEnumerable<StateTransition<StreamThreadStates>> validTransitions)
        {
            this.validTransitions = validTransitions
                .ToDictionary(k => k.StartingState, v => v);
        }

        public bool isRunning()
        {
            throw new NotImplementedException();
        }
    }
}
