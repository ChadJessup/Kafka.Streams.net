using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Tasks;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Threads.KafkaStream
{
    public class KafkaStreamThreadState : IStateMachine<KafkaStreamThreadStates>
    {
        private readonly object stateLock = new object();
        private readonly ILogger<KafkaStreamThreadState> logger;
        private Dictionary<KafkaStreamThreadStates, StateTransition<KafkaStreamThreadStates>> validTransitions = new Dictionary<KafkaStreamThreadStates, StateTransition<KafkaStreamThreadStates>>();
        private readonly string logPrefix = "";

        public KafkaStreamThreadState(
            ILogger<KafkaStreamThreadState> logger,
            StreamStateListener stateListener,
            TaskManager taskManager)
        {
            this.logger = logger;
            this.StateListener = stateListener;
            this.TaskManager = taskManager;
        }

        public KafkaStreamThreadStates CurrentState { get; protected set; }
        public IStateListener StateListener { get; }
        public TaskManager TaskManager { get; }
        public IThread<KafkaStreamThreadStates> Thread { get; }

        public bool isValidTransition(KafkaStreamThreadStates newState)
            => this.validTransitions.ContainsKey(newState)
                ? this.validTransitions[newState].PossibleTransitions.Contains(newState)
                : false;
        /**
         * Sets the state
         *
         * @param newState New state
         * @return The state prior to the call to setState, or null if the transition is invalid
         */
        public bool setState(KafkaStreamThreadStates newState)
        {
            KafkaStreamThreadStates oldState;

            lock (stateLock)
            {
                oldState = this.CurrentState;

                if (this.CurrentState == KafkaStreamThreadStates.PENDING_SHUTDOWN
                    && newState != KafkaStreamThreadStates.DEAD)
                {
                    logger.LogDebug("Ignoring request to transit from PENDING_SHUTDOWN to {}: " +
                                  "only DEAD state is a valid next state", newState);
                    // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                    // refused but we do not throw exception here
                    return false;
                }
                else if (this.CurrentState == KafkaStreamThreadStates.DEAD)
                {
                    logger.LogDebug("Ignoring request to transit from DEAD to {}: " +
                                  "no valid next state after DEAD", newState);
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return false;
                }
                else if (this.CurrentState == KafkaStreamThreadStates.PARTITIONS_REVOKED
                    && newState == KafkaStreamThreadStates.PARTITIONS_REVOKED)
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
                if (newState == KafkaStreamThreadStates.RUNNING)
                {
                    if (this.Thread is KafkaStreamThread st)
                    {
                        st.updateThreadMetadata(TaskManager.activeTasks(), TaskManager.standbyTasks());
                    }
                }
                else
                {
                    if (this.Thread is KafkaStreamThread st)
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

        public void setTransitions(IEnumerable<StateTransition<KafkaStreamThreadStates>> validTransitions)
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
