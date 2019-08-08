/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.Processor.Internals
{
    /**
     * This is the thread responsible for keeping all Global State Stores updated.
     * It delegates most of the responsibility to the internal StateConsumer
     */
    public class GlobalStreamThread // : Thread
    {


        private ILogger log;
        private LogContext logContext;
        private StreamsConfig config;
        private IConsumer<byte[], byte[]> globalConsumer;
        private StateDirectory stateDirectory;
        private ITime time;
        private ThreadCache cache;
        private StreamsMetricsImpl streamsMetrics;
        private ProcessorTopology topology;
        private volatile StreamsException startupException;

        /**
         * The states that the global stream thread can be in
         *
         * <pre>
         *                +-------------+
         *          +<--- | Created (0) |
         *          |     +-----+-------+
         *          |           |
         *          |           v
         *          |     +-----+-------+
         *          +<--- | Running (1) |
         *          |     +-----+-------+
         *          |           |
         *          |           v
         *          |     +-----+-------+
         *          +---> | Pending     |
         *                | Shutdown (2)|
         *                +-----+-------+
         *                      |
         *                      v
         *                +-----+-------+
         *                | Dead (3)    |
         *                +-------------+
         * </pre>
         *
         * Note the following:
         * <ul>
         *     <li>Any state can go to PENDING_SHUTDOWN. That is because streams can be closed at any time.</li>
         *     <li>State PENDING_SHUTDOWN may want to transit itself. In this case we will forbid the transition but will not treat as an error.</li>
         * </ul>
         */
        public enum State : IThreadStateTransitionValidator
        {

            CREATED(1, 2), RUNNING(2), PENDING_SHUTDOWN(3), DEAD;

        private HashSet<int> validTransitions = new HashSet<>();

        State(int[] validTransitions)
        {
            this.validTransitions.AddAll(Arrays.asList(validTransitions));
        }

        public bool isRunning()
        {
            return Equals(RUNNING);
        }


        public bool isValidTransition(IThreadStateTransitionValidator newState)
        {
            State tmpState = (State)newState;
            return validTransitions.contains(tmpState.ordinal());
        }

        private State state = State.CREATED;
        private object stateLock = new object();
        private StreamThread.StateListener stateListener = null;
        private string logPrefix;
        private IStateRestoreListener stateRestoreListener;

        /**
         * Set the {@link StreamThread.StateListener} to be notified when state changes. Note this API is internal to
         * Kafka Streams and is not intended to be used by an external application.
         */
        public void setStateListener(StreamThread.StateListener listener)
        {
            stateListener = listener;
        }

        /**
         * @return The state this instance is in
         */
        public State state()
        {
            // we do not need to use the stat lock since the variable is volatile
            return state;
        }

        /**
         * Sets the state
         *
         * @param newState New state
         */
        private void setState(State newState)
        {
            State oldState = state;

            synchronized(stateLock)
        {
                if (state == State.PENDING_SHUTDOWN && newState == State.PENDING_SHUTDOWN)
                {
                    // when the state is already in PENDING_SHUTDOWN, its transition to itself
                    // will be refused but we do not throw exception here
                    return;
                }
                else if (state == State.DEAD)
                {
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return;
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
            }

            if (stateListener != null)
            {
                stateListener.onChange(this, state, oldState);
            }
        }

        public bool stillRunning()
        {
            lock (stateLock)
            {
                return state.isRunning();
            }
        }

        public GlobalStreamThread(ProcessorTopology topology,
                                  StreamsConfig config,
                                  IConsumer<byte[], byte[]> globalConsumer,
                                  StateDirectory stateDirectory,
                                  long cacheSizeBytes,
                                  Metrics metrics,
                                  ITime time,
                                  string threadClientId,
                                  IStateRestoreListener stateRestoreListener)
            : base(threadClientId)
        {
            this.time = time;
            this.config = config;
            this.topology = topology;
            this.globalConsumer = globalConsumer;
            this.stateDirectory = stateDirectory;
            this.streamsMetrics = new StreamsMetricsImpl(metrics, threadClientId);
            this.logPrefix = string.Format("global-stream-thread [%s] ", threadClientId);
            this.logContext = new LogContext(logPrefix);
            this.log = logContext.logger(GetType());
            this.cache = new ThreadCache(logContext, cacheSizeBytes, streamsMetrics);
            this.stateRestoreListener = stateRestoreListener;
        }

        static class StateConsumer
        {

            private IConsumer<byte[], byte[]> globalConsumer;
            private GlobalStateMaintainer stateMaintainer;
            private ITime time;
            private TimeSpan pollTime;
            private long flushInterval;
            private ILogger log;

            private long lastFlush;

            StateConsumer(LogContext logContext,
                          IConsumer<byte[], byte[]> globalConsumer,
                          GlobalStateMaintainer stateMaintainer,
                          ITime time,
                          TimeSpan pollTime,
                          long flushInterval)
            {
                this.log = logContext.logger(GetType());
                this.globalConsumer = globalConsumer;
                this.stateMaintainer = stateMaintainer;
                this.time = time;
                this.pollTime = pollTime;
                this.flushInterval = flushInterval;
            }

            /**
             * @throws InvalidOperationException If store gets registered after initialized is already finished
             * @throws StreamsException      if the store's change log does not contain the partition
             */
            void initialize()
            {
                Dictionary<TopicPartition, long> partitionOffsets = stateMaintainer.initialize();
                globalConsumer.assign(partitionOffsets.Keys);
                foreach (KeyValuePair<TopicPartition, long> entry in partitionOffsets)
                {
                    globalConsumer.seek(entry.Key, entry.Value);
                }
                lastFlush = time.milliseconds();
            }

            void pollAndUpdate()
            {
                try
                {

                    ConsumerRecords<byte[], byte[]> received = globalConsumer.poll(pollTime);
                    foreach (ConsumeResult<byte[], byte[]> record in received)
                    {
                        stateMaintainer.update(record);
                    }
                    long now = time.milliseconds();
                    if (now >= lastFlush + flushInterval)
                    {
                        stateMaintainer.flushState();
                        lastFlush = now;
                    }
                }
                catch (InvalidOffsetException recoverableException)
                {
                    log.LogError("Updating global state failed. You can restart KafkaStreams to recover from this error.", recoverableException);
                    throw new StreamsException("Updating global state failed. " +
                        "You can restart KafkaStreams to recover from this error.", recoverableException);
                }
            }

            public void close()
            {
                try
                {

                    globalConsumer.close();
                }
                catch (RuntimeException e)
                {
                    // just log an error if the consumerclose
                    // so we can always attempt to close the state stores.
                    log.LogError("Failed to close global consumer due to the following error:", e);
                }

                stateMaintainer.close();
            }
        }


        public void run()
        {
            StateConsumer stateConsumer = initialize();

            if (stateConsumer == null)
            {
                // during initialization, the caller thread would wait for the state consumer
                // to restore the global state store before transiting to RUNNING state and return;
                // if an error happens during the restoration process, the stateConsumer will be null
                // and in this case we will transit the state to PENDING_SHUTDOWN and DEAD immediately.
                // the exception will be thrown in the caller thread during start() function.
                setState(State.PENDING_SHUTDOWN);
                setState(State.DEAD);

                log.LogWarning("Error happened during initialization of the global state store; this thread has shutdown");
                streamsMetrics.removeAllThreadLevelSensors();

                return;
            }
            setState(State.RUNNING);

            try
            {

                while (stillRunning())
                {
                    stateConsumer.pollAndUpdate();
                }
            }
            finally
            {

                // set the state to pending shutdown first as it may be called due to error;
                // its state may already be PENDING_SHUTDOWN so it will return false but we
                // intentionally do not check the returned flag
                setState(State.PENDING_SHUTDOWN);

                log.LogInformation("Shutting down");

                try
                {

                    stateConsumer.close();
                }
                catch (IOException e)
                {
                    log.LogError("Failed to close state maintainer due to the following error:", e);
                }

                streamsMetrics.removeAllThreadLevelSensors();

                setState(DEAD);

                log.LogInformation("Shutdown complete");
            }
        }

        private StateConsumer initialize()
        {
            try
            {

                GlobalStateManager stateMgr = new GlobalStateManagerImpl(
                    logContext,
                    topology,
                    globalConsumer,
                    stateDirectory,
                    stateRestoreListener,
                    config);

                GlobalProcessorContextImpl globalProcessorContext = new GlobalProcessorContextImpl(
                    config,
                    stateMgr,
                    streamsMetrics,
                    cache);
                stateMgr.setGlobalProcessorContext(globalProcessorContext);

                StateConsumer stateConsumer = new StateConsumer(
                    logContext,
                    globalConsumer,
                    new GlobalStateUpdateTask(
                        topology,
                        globalProcessorContext,
                        stateMgr,
                        config.defaultDeserializationExceptionHandler(),
                        logContext
                    ),
                    time,
                    Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG)),
                    config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG)
                );
                stateConsumer.initialize();

                return stateConsumer;
            }
            catch (LockException fatalException)
            {
                string errorMsg = "Could not lock global state directory. This could happen if multiple KafkaStreams " +
                    "instances are running on the same host using the same state directory.";
                log.LogError(errorMsg, fatalException);
                startupException = new StreamsException(errorMsg, fatalException);
            }
            catch (StreamsException fatalException)
            {
                startupException = fatalException;
            }
            catch (Exception fatalException)
            {
                startupException = new StreamsException("Exception caught during initialization of GlobalStreamThread", fatalException);
            }
            return null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void start()
        {
            base.start();
            while (!stillRunning())
            {
                Utils.sleep(1);
                if (startupException != null)
                {
                    throw startupException;
                }
            }
        }

        public void shutdown()
        {
            // one could call shutdown() multiple times, so ignore subsequent calls
            // if already shutting down or dead
            setState(PENDING_SHUTDOWN);
        }

        public Dictionary<MetricName, Metric> consumerMetrics()
        {
            return Collections.unmodifiableMap(globalConsumer.metrics());
        }
    }
