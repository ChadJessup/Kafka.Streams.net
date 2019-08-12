using Confluent.Kafka;
using Kafka.Common.Metrics;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class StreamThread //: Thread
    {
        private HashSet<int> validTransitions = new HashSet<int>();

        public StreamThread(int[] validTransitions)
        {
            this.validTransitions.UnionWith(validTransitions);
        }

        public bool isRunning()
        {
            return true; // Equals(RUNNING) || Equals(STARTING) || Equals(PARTITIONS_REVOKED) || Equals(PARTITIONS_ASSIGNED);
        }


        public bool isValidTransition(IThreadStateTransitionValidator newState)
        {
            StreamThreadState tmpState = (StreamThreadState)newState;
            return validTransitions.Contains((int)tmpState);
        }

        internal static StreamThread create(InternalTopologyBuilder internalTopologyBuilder, StreamsConfig config, IKafkaClientSupplier clientSupplier, IAdminClient adminClient, Guid processId, string clientId, MetricsRegistry metrics, ITime time, StreamsMetadataState streamsMetadataState, long cacheSizePerThread, StateDirectory stateDirectory, IStateRestoreListener delegatingStateRestoreListener, int v)
        {
            throw new NotImplementedException();
        }

        internal long getId()
        {
            throw new NotImplementedException();
        }

        internal StreamThreadState state()
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

        internal ThreadMetadata threadMetadata()
        {
            throw new NotImplementedException();
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
        public StreamThreadState setState(StreamThreadState newState)
        {
            StreamThreadState oldState;

            lock (stateLock)
            {
                oldState = state;

                if (state == StreamThreadState.PENDING_SHUTDOWN && newState != StreamThreadState.DEAD)
                {
                    log.LogDebug("Ignoring request to transit from PENDING_SHUTDOWN to {}: " +
                                  "only DEAD state is a valid next state", newState);
                    // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                    // refused but we do not throw exception here
                    return null;
                }
                else if (state == StreamThreadState.DEAD)
                {
                    log.LogDebug("Ignoring request to transit from DEAD to {}: " +
                                  "no valid next state after DEAD", newState);
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return null;
                }
                else if (state == StreamThreadState.PARTITIONS_REVOKED && newState == StreamThreadState.PARTITIONS_REVOKED)
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

                    log.LogInformation("StreamThreadState transition from {} to {}", oldState, newState);
                }

                state = newState;
                if (newState == StreamThreadState.RUNNING)
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
            return state == StreamThreadState.RUNNING;
        }

        public bool isRunning()
        {
            lock (stateLock)
            {
                return state.isRunning();
            }
        }
    }
}