using Confluent.Kafka;
using Kafka.Common.Metrics;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.IProcessor.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.IProcessor.Internals
{
    public class StreamThread //: Thread
    {
        /**
         * Stream thread states are the possible states that a stream thread can be in.
         * A thread must only be in one state at a time
         * The expected state transitions with the following defined states is:
         *
         * <pre>
         *                +-------------+
         *          +<--- | Created (0) |
         *          |     +-----+-------+
         *          |           |
         *          |           v
         *          |     +-----+-------+
         *          +<--- | Starting (1)|
         *          |     +-----+-------+
         *          |           |
         *          |           |
         *          |           v
         *          |     +-----+-------+
         *          +<--- | Partitions  |
         *          |     | Revoked (2) | <----+
         *          |     +-----+-------+      |
         *          |           |              |
         *          |           v              |
         *          |     +-----+-------+      |
         *          |     | Partitions  |      |
         *          +<--- | Assigned (3)| ---=>+
         *          |     +-----+-------+      |
         *          |           |              |
         *          |           v              |
         *          |     +-----+-------+      |
         *          |     | Running (4) | ---=>+
         *          |     +-----+-------+
         *          |           |
         *          |           v
         *          |     +-----+-------+
         *          +--=> | Pending     |
         *                | Shutdown (5)|
         *                +-----+-------+
         *                      |
         *                      v
         *                +-----+-------+
         *                | Dead (6)    |
         *                +-------------+
         * </pre>
         *
         * Note the following:
         * <ul>
         *     <li>Any state can go to PENDING_SHUTDOWN. That is because streams can be closed at any time.</li>
         *     <li>
         *         State PENDING_SHUTDOWN may want to transit to some other states other than DEAD,
         *         in the corner case when the shutdown is triggered while the thread is still in the rebalance loop.
         *         In this case we will forbid the transition but will not treat as an error.
         *     </li>
         *     <li>
         *         State PARTITIONS_REVOKED may want transit to itself indefinitely, in the corner case when
         *         the coordinator repeatedly fails in-between revoking partitions and assigning new partitions.
         *         Also during streams instance start up PARTITIONS_REVOKED may want to transit to itself as well.
         *         In this case we will forbid the transition but will not treat as an error.
         *     </li>
         * </ul>
         */
        public class State : IThreadStateTransitionValidator
        {

            //            CREATED(1, 5), STARTING(2, 5), PARTITIONS_REVOKED(3, 5), PARTITIONS_ASSIGNED(2, 4, 5), RUNNING(2, 5), PENDING_SHUTDOWN(6), DEAD;
            public static State DEAD { get; internal set; }
        }

        private HashSet<int> validTransitions = new HashSet<int>();

        State(int[] validTransitions)
        {
            this.validTransitions.UnionWith(validTransitions);
        }

        public bool isRunning()
        {
            return true; // Equals(RUNNING) || Equals(STARTING) || Equals(PARTITIONS_REVOKED) || Equals(PARTITIONS_ASSIGNED);
        }


        public bool isValidTransition(IThreadStateTransitionValidator newState)
        {
            State tmpState = (State)newState;
            return validTransitions.Contains(tmpState);
        }

        internal static StreamThread create(InternalTopologyBuilder internalTopologyBuilder, StreamsConfig config, IKafkaClientSupplier clientSupplier, IAdminClient adminClient, Guid processId, string clientId, MetricsRegistry metrics, ITime time, StreamsMetadataState streamsMetadataState, long cacheSizePerThread, StateDirectory stateDirectory, IStateRestoreListener delegatingStateRestoreListener, int v)
        {
            throw new NotImplementedException();
        }

        internal long getId()
        {
            throw new NotImplementedException();
        }

        internal State state()
        {
            throw new NotImplementedException();
        }

        internal object tasks()
        {
            throw new NotImplementedException();
        }

        internal bool isRunningAndNotRebalancing()
        {
            throw new NotImplementedException();
        }

        internal static string getSharedAdminClientId(string clientId)
        {
            throw new NotImplementedException();
        }
    }

    /**
     * Set the {@link StreamThread.StateListener} to be notified when state changes. Note this API is internal to
     * Kafka Streams and is not intended to be used by an external application.
     */
    public void setStateListener(IStateListener listener)
    {
        stateListener = listener;
    }

    /**
     * Sets the state
     *
     * @param newState New state
     * @return The state prior to the call to setState, or null if the transition is invalid
     */
    State setState(State newState)
    {
        State oldState;

        lock (stateLock)
        {
            oldState = state;

            if (state == State.PENDING_SHUTDOWN && newState != State.DEAD)
            {
                log.LogDebug("Ignoring request to transit from PENDING_SHUTDOWN to {}: " +
                              "only DEAD state is a valid next state", newState);
                // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                // refused but we do not throw exception here
                return null;
            }
            else if (state == State.DEAD)
            {
                log.LogDebug("Ignoring request to transit from DEAD to {}: " +
                              "no valid next state after DEAD", newState);
                // when the state is already in NOT_RUNNING, all its transitions
                // will be refused but we do not throw exception here
                return null;
            }
            else if (state == State.PARTITIONS_REVOKED && newState == State.PARTITIONS_REVOKED)
            {
                log.LogDebug("Ignoring request to transit from PARTITIONS_REVOKED to PARTITIONS_REVOKED: " +
                              "self transition is not allowed");
                // when the state is already in PARTITIONS_REVOKED, its transition to itself will be
                // refused but we do not throw exception here
                return null;
            }
            else if (!state.isValidTransition(newState))
            {
                log.LogError("Unexpected state transition from {} to {}", oldState, newState);
                throw new StreamsException(logPrefix + "Unexpected state transition from " + oldState + " to " + newState);
            }
            else
            {

                log.LogInformation("State transition from {} to {}", oldState, newState);
            }

            state = newState;
            if (newState == State.RUNNING)
            {
                updateThreadMetadata(taskManager.activeTasks(), taskManager.standbyTasks());
            }
            else
            {

                updateThreadMetadata(Collections.emptyMap(), Collections.emptyMap());
            }
        }

        if (stateListener != null)
        {
            stateListener.onChange(this, state, oldState);
        }

        return oldState;
    }

    public bool isRunningAndNotRebalancing()
    {
        // we do not need to grab stateLock since it is a single read
        return state == State.RUNNING;
    }

    public bool isRunning()
    {
        lock (stateLock)
        {
            return state.isRunning();
        }
    }
}