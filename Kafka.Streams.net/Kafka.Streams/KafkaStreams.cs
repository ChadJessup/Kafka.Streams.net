/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
using Confluent.Kafka;
using Kafka.Common;
using Kafka.Common.Interfaces;
using Kafka.Common.Metrics;
using Kafka.Common.Metrics.Interfaces;
using Kafka.Common.Utils;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Internals.Kafka.Streams.Internals;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processor;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Kafka.Streams
{
    /**
     * A Kafka client that allows for performing continuous computation on input coming from one or more input topics and
     * sends output to zero, one, or more output topics.
     * <p>
     * The computational logic can be specified either by using the {@link Topology} to define a DAG topology of
     * {@link IProcessor}s or by using the {@link StreamsBuilder} which provides the high-level DSL to define
     * transformations.
     * <p>
     * One {@code KafkaStreams} instance can contain one or more threads specified in the configs for the processing work.
     * <p>
     * A {@code KafkaStreams} instance can co-ordinate with any other instances with the same
     * {@link StreamsConfig#APPLICATION_ID_CONFIG application ID} (whether in the same process, on other processes on this
     * machine, or on remote machines) as a single (possibly distributed) stream processing application.
     * These instances will divide up the work based on the assignment of the input topic partitions so that all partitions
     * are being consumed.
     * If instances are added or fail, all (remaining) instances will rebalance the partition assignment among themselves
     * to balance processing load and ensure that all input topic partitions are processed.
     * <p>
     * Internally a {@code KafkaStreams} instance Contains a normal {@link KafkaProducer} and {@link KafkaConsumer} instance
     * that is used for reading input and writing output.
     * <p>
     * A simple example might look like this:
     * <pre>{@code
     * Properties props = new Properties();
     * props.Add(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
     * props.Add(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
     * props.Add(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.string().getClass());
     * props.Add(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.string().getClass());
     *
     * StreamsBuilder builder = new StreamsBuilder();
     * builder.<string, string>stream("my-input-topic").mapValues(value => string.valueOf(value.Length())).to("my-output-topic");
     *
     * KafkaStreams streams = new KafkaStreams(builder.build(), props);
     * streams.start();
     * }</pre>
     *
     * @see org.apache.kafka.streams.StreamsBuilder
     * @see org.apache.kafka.streams.Topology
     */
    public class KafkaStreams : IDisposable
    {
        private static string JMX_PREFIX = "kafka.streams";

        // processId is expected to be unique across JVMs and to be used
        // in userData of the subscription request to allow assignor be aware
        // of the co-location of stream thread's consumers. It is for internal
        // usage only and should not be exposed to users at all.
        private ITime time;
        private ILogger log;
        private string clientId;
        private MetricsRegistry metrics;
        private StreamsConfig config;
        //protected StreamThread[] threads;
        //private StateDirectory stateDirectory;
        private StreamsMetadataState streamsMetadataState;
        //        private ScheduledExecutorService stateDirCleaner;
        //private QueryableStoreProvider queryableStoreProvider;
        private IAdminClient adminClient;

        //private GlobalStreamThread globalStreamThread;
        private IStateListener stateListener;
        private IStateRestoreListener globalStateRestoreListener;

        private object stateLock = new object();
        protected KafkaStreamsState state = new KafkaStreamsState(KafkaStreamsStates.CREATED);

        /**
 * Create a {@code KafkaStreams} instance.
 * <p>
 * Note: even if you never call {@link #start()} on a {@code KafkaStreams} instance,
 * you still must {@link #close()} it to avoid resource leaks.
 *
 * @param topology the topology specifying the computational logic
 * @param props    properties for {@link StreamsConfig}
 * @throws StreamsException if any fatal error occurs
 */
        public KafkaStreams(
            Topology topology,
            StreamsConfig config)
            : this(topology.internalTopologyBuilder,
                  config,
                  new DefaultKafkaClientSupplier())
        {
        }

        /**
         * Create a {@code KafkaStreams} instance.
         * <p>
         * Note: even if you never call {@link #start()} on a {@code KafkaStreams} instance,
         * you still must {@link #close()} it to avoid resource leaks.
         *
         * @param topology       the topology specifying the computational logic
         * @param props          properties for {@link StreamsConfig}
         * @param clientSupplier the Kafka clients supplier which provides underlying producer and consumer clients
         *                       for the new {@code KafkaStreams} instance
         * @throws StreamsException if any fatal error occurs
         */
        public KafkaStreams(
            Topology topology,
            StreamsConfig config,
            IKafkaClientSupplier clientSupplier)
            : this(
                  topology.internalTopologyBuilder,
                  config,
                  clientSupplier,
                  Time.SYSTEM)
        {
        }

        /**
         * Create a {@code KafkaStreams} instance.
         * <p>
         * Note: even if you never call {@link #start()} on a {@code KafkaStreams} instance,
         * you still must {@link #close()} it to avoid resource leaks.
         *
         * @param topology       the topology specifying the computational logic
         * @param props          properties for {@link StreamsConfig}
         * @param time           {@code Time} implementation; cannot be null
         * @throws StreamsException if any fatal error occurs
         */
        public KafkaStreams(
            Topology topology,
            StreamsConfig config,
            Time time)
            : this(
                  topology.internalTopologyBuilder,
                  config,
                  new DefaultKafkaClientSupplier(),
                  time)
        {
        }

        /**
         * Create a {@code KafkaStreams} instance.
         * <p>
         * Note: even if you never call {@link #start()} on a {@code KafkaStreams} instance,
         * you still must {@link #close()} it to avoid resource leaks.
         *
         * @param topology       the topology specifying the computational logic
         * @param props          properties for {@link StreamsConfig}
         * @param clientSupplier the Kafka clients supplier which provides underlying producer and consumer clients
         *                       for the new {@code KafkaStreams} instance
         * @param time           {@code Time} implementation; cannot be null
         * @throws StreamsException if any fatal error occurs
         */
        public KafkaStreams(
            Topology topology,
            StreamsConfig config,
            IKafkaClientSupplier clientSupplier,
            Time time)
            : this(
                  topology.internalTopologyBuilder,
                  config,
                  clientSupplier,
                  time)
        {
        }

        /**
         * @deprecated use {@link #KafkaStreams(Topology, Properties)} instead
         */
        //public KafkaStreams(Topology topology,
        //                     StreamsConfig config)
        //    {
        //        this(topology, config, new DefaultKafkaClientSupplier());
        //    }

        /**
         * @deprecated use {@link #KafkaStreams(Topology, Properties, IKafkaClientSupplier)} instead
         */
        //public KafkaStreams(Topology topology,
        //                     StreamsConfig config,
        //                     IKafkaClientSupplier clientSupplier)
        //    {
        //        this(topology.internalTopologyBuilder, config, clientSupplier);
        //    }

        /**
         * @deprecated use {@link #KafkaStreams(Topology, Properties, Time)} instead
         */
        //public KafkaStreams(Topology topology,
        //                     StreamsConfig config,
        //                     Time time)
        //    {
        //        this(topology.internalTopologyBuilder, config, new DefaultKafkaClientSupplier(), time);
        //    }

        private KafkaStreams(
            InternalTopologyBuilder internalTopologyBuilder,
            StreamsConfig config,
            IKafkaClientSupplier clientSupplier)
            : this(
                  internalTopologyBuilder,
                  config,
                  clientSupplier,
                  Time.SYSTEM)
        {
        }

        private KafkaStreams(
            InternalTopologyBuilder internalTopologyBuilder,
            StreamsConfig config,
            IKafkaClientSupplier clientSupplier,
            ITime time)
        {
            this.config = config;
            this.time = time;

            // The application ID is a required config and hence should always have value
            var processId = Guid.NewGuid();

            string userClientId = config.Get(StreamsConfigPropertyNames.ClientId) ?? "";
            string applicationId = config.Get(StreamsConfigPropertyNames.ApplicationId) ?? "";

            if (userClientId.Length <= 0)
            {
                clientId = applicationId + "-" + processId;
            }
            else
            {
                clientId = userClientId;
            }

            LogContext logContext = new LogContext(string.Format("stream-client [%s] ", clientId));
            this.log = logContext.logger(GetType());

            //MetricConfig metricConfig = new MetricConfig()
            //    .samples(config.GetInt(StreamsConfig.METRICS_NUM_SAMPLES_CONFIG))
            //    .recordLevel(RecordingLevel.forName(config.getString(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG)))
            //    .timeWindow(config.getLong(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS);
            //List<IMetricsReporter> reporters = config.getConfiguredInstances(StreamsConfig.METRIC_REPORTER_CLASSES_CONFIG,
            //        typeof(IMetricsReporter),
            //    StreamsConfig.CLIENT_ID_CONFIG, clientId);
            //reporters.Add(new JmxReporter(JMX_PREFIX));
            //metrics = new Metrics(metricConfig, reporters, time);

            // re-write the physical topology according to the config
            internalTopologyBuilder.rewriteTopology(config);

            // sanity check to fail-fast in case we cannot build a ProcessorTopology due to an exception
            //            var taskTopology = internalTopologyBuilder.build();

            //            streamsMetadataState = new StreamsMetadataState(
            //                    internalTopologyBuilder,
            //                    parseHostInfo(config.Get(StreamsConfigPropertyNames.ApplicationServer)));

            //            // create the stream thread, global update thread, and cleanup thread
            //            threads = new StreamThread[config.GetInt(StreamsConfigPropertyNames.NUM_STREAM_THREADS_CONFIG).Value];

            //            long totalCacheSize = config.getLong(StreamsConfigPropertyNames.CACHE_MAX_BYTES_BUFFERING_CONFIG).Value;
            //            if (totalCacheSize < 0)
            //            {
            //                totalCacheSize = 0;
            //                log.LogWarning("Negative cache size passed in. Reverting to cache size of 0 bytes.");
            //            }

            //            var globalTaskTopology = internalTopologyBuilder.buildGlobalStateTopology();
            //            long cacheSizePerThread = totalCacheSize / (threads.Length + (globalTaskTopology == null ? 0 : 1));
            //            bool createStateDirectory = taskTopology.hasPersistentLocalStore()
            //                || (globalTaskTopology != null && globalTaskTopology.hasPersistentGlobalStore());

            //            try
            //            {
            //                stateDirectory = new StateDirectory(config, time, createStateDirectory);
            //            }
            //            catch (ProcessorStateException fatal)
            //            {
            //                throw new StreamsException(fatal);
            //            }

            //            IStateRestoreListener delegatingStateRestoreListener = new DelegatingStateRestoreListener();
            //            //GlobalStreamThread.State globalThreadState = null;
            //            if (globalTaskTopology != null)
            //            {
            //                string globalThreadId = clientId + "-GlobalStreamThread";
            //                //globalStreamThread = new GlobalStreamThread(
            //                //    globalTaskTopology,
            //                //    config,
            //                //    clientSupplier.getGlobalConsumer(config.GetGlobalConsumerConfigs(clientId)),
            //                //    stateDirectory,
            //                //    cacheSizePerThread,
            //                //    metrics,
            //                //    time,
            //                //    globalThreadId,
            //                //    delegatingStateRestoreListener);

            ////                globalThreadState = globalStreamThread.state();
            //            }

            // use client id instead of thread client id since this admin client may be shared among threads
            //            adminClient = clientSupplier.getAdminClient(config.getAdminConfigs(StreamThread.getSharedAdminClientId(clientId)));

            //Dictionary<long, StreamThread.StreamThreadState> threadState = new Dictionary<long, StreamThread.StreamThreadState>(threads.Length);
            //List<IStateStoreProvider> storeProviders = new List<IStateStoreProvider>();
            //for (int i = 0; i < threads.Length; i++)
            //{
            //    threads[i] = StreamThread.create(
            //        internalTopologyBuilder,
            //        config,
            //        clientSupplier,
            //        adminClient,
            //        processId,
            //        clientId,
            //        metrics,
            //        time,
            //        streamsMetadataState,
            //        cacheSizePerThread,
            //        stateDirectory,
            //        delegatingStateRestoreListener,
            //        i + 1);

            //    threadState.Add(threads[i].getId(), threads[i].state());
            //    storeProviders.Add(new StreamThreadStateStoreProvider(threads[i]));
            //}

            //StreamStateListener streamStateListener = new StreamStateListener(threadState, globalThreadState);
            //if (globalTaskTopology != null)
            //{
            //    globalStreamThread.setStateListener(streamStateListener);
            //}
            //        foreach (StreamThread thread in threads) {
            //            thread.setStateListener(streamStateListener);
            //        }

            //        GlobalStateStoreProvider globalStateStoreProvider = new GlobalStateStoreProvider(internalTopologyBuilder.globalStateStores());
            //queryableStoreProvider = new QueryableStoreProvider(storeProviders, globalStateStoreProvider);

            //stateDirCleaner = Executors.newSingleThreadScheduledExecutor(r => {
            //            Thread thread = new Thread(r, clientId + "-CleanupThread");
            //thread.setDaemon(true);
            //            return thread;
            //        });
        }
        private bool waitOnState(KafkaStreamsStates targetState, long waitMs)
        {
            long begin = time.milliseconds();
            lock (stateLock)
            {
                long elapsedMs = 0L;
                while (state.CurrentState != targetState)
                {
                    if (waitMs > elapsedMs)
                    {
                        long remainingMs = waitMs - elapsedMs;
                        try
                        {
                            // stateLock.wait(remainingMs);
                        }
                        catch (Exception e)
                        {
                            // it is ok: just move on to the next iteration
                        }
                    }
                    else
                    {
                        log.LogDebug("Cannot transit to {} within {}ms", targetState, waitMs);
                        return false;
                    }

                    elapsedMs = time.milliseconds() - begin;
                }

                return true;
            }
        }

        /**
         * Sets the state
         * @param newState New state
         */
        private bool setState(KafkaStreamsStates newState)
        {
            KafkaStreamsStates oldState;

            lock (stateLock)
            {
                oldState = state.CurrentState;

                if (state.CurrentState == KafkaStreamsStates.PENDING_SHUTDOWN && newState != KafkaStreamsStates.NOT_RUNNING)
                {
                    // when the state is already in PENDING_SHUTDOWN, all other transitions than NOT_RUNNING (due to thread dying) will be
                    // refused but we do not throw exception here, to allow appropriate error handling
                    return false;
                }
                else if (state.CurrentState == KafkaStreamsStates.NOT_RUNNING
                    && (newState == KafkaStreamsStates.PENDING_SHUTDOWN
                    || newState == KafkaStreamsStates.NOT_RUNNING))
                {
                    // when the state is already in NOT_RUNNING, its transition to PENDING_SHUTDOWN or NOT_RUNNING (due to consecutive close calls)
                    // will be refused but we do not throw exception here, to allow idempotent close calls
                    return false;
                }
                else if (state.CurrentState == KafkaStreamsStates.REBALANCING
                    && newState == KafkaStreamsStates.REBALANCING)
                {
                    // when the state is already in REBALANCING, it should not transit to REBALANCING again
                    return false;
                }
                else if (state.CurrentState == KafkaStreamsStates.ERROR
                    && newState == KafkaStreamsStates.ERROR)
                {
                    // when the state is already in ERROR, it should not transit to ERROR again
                    return false;
                }
                else if (!state.isValidTransition(newState))
                {
                    throw new Exception("Stream-client " + clientId + ": Unexpected state transition from " + oldState + " to " + newState);
                }
                else
                {
                    log.LogInformation("State transition from {} to {}", oldState, newState);
                }

                state.CurrentState = newState;
                //stateLock.notifyAll();
            }

            // we need to call the user customized state listener outside the state lock to avoid potential deadlocks
            if (stateListener != null)
            {
                stateListener.onChange(newState, oldState);
            }

            return true;
        }

        private bool isRunning()
        {
            lock (stateLock)
            {
                return state.isRunning();
            }
        }

        private void validateIsRunning()
        {
            if (!isRunning())
            {
                throw new Exception("KafkaStreams is not running. State is " + state + ".");
            }
        }

        /**
         * An app can set a single {@link KafkaStreams.StateListener} so that the app is notified when state changes.
         *
         * @param listener a new state listener
         * @throws Exception if this {@code KafkaStreams} instance is not in state {@link State#CREATED CREATED}.
         */
        public void setStateListener(IStateListener listener)
        {
            lock (stateLock)
            {
                if (state.CurrentState == KafkaStreamsStates.CREATED)
                {
                    stateListener = listener;
                }
                else
                {
                    throw new Exception("Can only set StateListener in CREATED state. Current state is: " + state);
                }
            }
        }

        /**
         * Set the handler invoked when a {@link StreamsConfig#NUM_STREAM_THREADS_CONFIG internal thread} abruptly
         * terminates due to an uncaught exception.
         *
         * @param eh the uncaught exception handler for all internal threads; {@code null} deletes the current handler
         * @throws Exception if this {@code KafkaStreams} instance is not in state {@link State#CREATED CREATED}.
         */
        public void setUncaughtExceptionHandler(/*UncaughtExceptionHandler eh*/)
        {
            lock (stateLock)
            {
                if (state.CurrentState == KafkaStreamsStates.CREATED)
                {
                    //foreach (StreamThread thread in threads)
                    {
                        // thread.setUncaughtExceptionHandler(eh);
                    }

                    //if (globalStreamThread != null)
                    {
                        //                        globalStreamThread.setUncaughtExceptionHandler(eh);
                    }
                }
                else
                {
                    throw new Exception("Can only set UncaughtExceptionHandler in CREATED state. " +
                        "Current state is: " + state);
                }
            }
        }

        /**
         * Set the listener which is triggered whenever a {@link StateStore} is being restored in order to resume
         * processing.
         *
         * @param globalStateRestoreListener The listener triggered when {@link StateStore} is being restored.
         * @throws Exception if this {@code KafkaStreams} instance is not in state {@link State#CREATED CREATED}.
         */
        public void setGlobalStateRestoreListener(IStateRestoreListener globalStateRestoreListener)
        {
            lock (stateLock)
            {
                if (state.CurrentState == KafkaStreamsStates.CREATED)
                {
                    this.globalStateRestoreListener = globalStateRestoreListener;
                }
                else
                {
                    throw new Exception("Can only set GlobalStateRestoreListener in CREATED state. " +
                        "Current state is: " + state);
                }
            }
        }

        /**
         * Get read-only handle on global metrics registry, including streams client's own metrics plus
         * its embedded producer, consumer and admin clients' metrics.
         *
         * @return Map of all metrics.
         */
        public Dictionary<MetricName, IMetric> GetMetrics()
        {
            var result = new Dictionary<MetricName, IMetric>();
            // producer and consumer clients are per-thread
//            foreach (StreamThread thread in threads)
            {
                //result.putAll(thread.producerMetrics());
                //result.putAll(thread.consumerMetrics());
                // admin client is shared, so we can actually move it
                // to result.putAll(adminClient.metrics).
                // we did it intentionally just for flexibility.
                //result.putAll(thread.adminClientMetrics());
            }
            // global thread's consumer client
            //if (globalStreamThread != null)
            //{
            //    //result.putAll(globalStreamThread.consumerMetrics());
            //}
            // self streams metrics
            //            result.putAll(metrics.metrics);
            return result;
        }

        private static HostInfo parseHostInfo(string endPoint)
        {
            if (endPoint == null || !endPoint.Trim().Any())
            {
                return StreamsMetadataState.UNKNOWN_HOST;
            }

            string host = getHost(endPoint);
            int port = getPort(endPoint);

            if (host == null || port == null)
            {
                throw new Exception(string.Format("Error parsing host address %s. Expected string.Format host:port.", endPoint));
            }

            return new HostInfo(host, port);
        }

        private static int getPort(string endPoint)
        {
            throw new NotImplementedException();
        }

        private static string getHost(string endPoint)
        {
            throw new NotImplementedException();
        }

        /**
         * Start the {@code KafkaStreams} instance by starting all its threads.
         * This function is expected to be called only once during the life cycle of the client.
         * <p>
         * Because threads are started in the background, this method does not block.
         * However, if you have global stores in your topology, this method blocks until all global stores are restored.
         * As a consequence, any fatal exception that happens during processing is by default only logged.
         * If you want to be notified about dying threads, you can
         * {@link #setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler) register an uncaught exception handler}
         * before starting the {@code KafkaStreams} instance.
         * <p>
         * Note, for brokers with version {@code 0.9.x} or lower, the broker version cannot be checked.
         * There will be no error and the client will hang and retry to verify the broker version until it
         * {@link StreamsConfig#REQUEST_TIMEOUT_MS_CONFIG times out}.

         * @throws IllegalStateException if process was already started
         * @throws StreamsException if the Kafka brokers have version 0.10.0.x or
         *                          if {@link StreamsConfig#PROCESSING_GUARANTEE_CONFIG exactly-once} is enabled for pre 0.11.0.x brokers
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void start()
        {
            //if (setState(State.REBALANCING))
            //{
            //    log.LogDebug("Starting Streams client");

            //    if (globalStreamThread != null)
            //    {
            //        globalStreamThread.start();
            //    }

            //    //foreach (StreamThread thread in threads)
            //    //{
            //    //    thread.start();
            //    //}

            //    long cleanupDelay = config.getLong(StreamConfigPropertyNames.STATE_CLEANUP_DELAY_MS_CONFIG).Value;
            //    //stateDirCleaner.scheduleAtFixedRate(()=> {
            //    //    // we do not use lock here since we only read on the value and act on it
            //    //    if (state == State.RUNNING)
            //    //    {
            //    //        stateDirectory.cleanRemovedTasks(cleanupDelay);
            //    //    }
            //    //}, cleanupDelay, cleanupDelay, TimeUnit.MILLISECONDS);
            //}
            //else
            //{
            //    throw new Exception("The client is either already started or already stopped, cannot re-start");
            //}
        }

        /**
         * Shutdown this {@code KafkaStreams} instance by signaling all the threads to stop, and then wait for them to join.
         * This will block until all threads have stopped.
         */
        public void close()
        {
            close(long.MaxValue);
        }

        /**
         * Shutdown this {@code KafkaStreams} by signaling all the threads to stop, and then wait up to the timeout for the
         * threads to join.
         * A {@code timeout} of 0 means to wait forever.
         *
         * @param timeout  how long to wait for the threads to shutdown. Can't be negative. If {@code timeout=0} just checking the state and return immediately.
         * @param timeUnit unit of time used for timeout
         * @return {@code true} if all threads were successfully stopped&mdash;{@code false} if the timeout was reached
         * before all threads stopped
         * Note that this method must not be called in the {@code onChange} callback of {@link StateListener}.
         * @deprecated Use {@link #close(Duration)} instead; note, that {@link #close(Duration)} has different semantics and does not block on zero, e.g., `Duration.ofMillis(0)`.
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool close(long timeout, TimeUnit timeUnit)
        {
            long timeoutMs = 0; // timeUnit.toMillis(timeout);

            log.LogDebug("Stopping Streams client with timeoutMillis = {} ms. You are using deprecated method. " +
                "Please, consider update your code.", timeoutMs);

            if (timeoutMs < 0)
            {
                timeoutMs = 0;
            }
            else if (timeoutMs == 0)
            {
                timeoutMs = long.MaxValue;
            }

            return close(timeoutMs);
        }

        private bool close(long timeoutMs)
        {
            return true;

            //if (!setState(State.PENDING_SHUTDOWN))
            //{
            //    // if transition failed, it means it was either in PENDING_SHUTDOWN
            //    // or NOT_RUNNING already; just check that all threads have been stopped
            //    log.LogInformation("Already in the pending shutdown state, wait to complete shutdown");
            //}
            //else
            //{
            //    stateDirCleaner.shutdownNow();

            //    // wait for all threads to join in a separate thread;
            //    // save the current thread so that if it is a stream thread
            //    // we don't attempt to join it and cause a deadlock
            //    //        Thread shutdownThread = new Thread(new ParameterizedThreadStart(() =>
            //    //        {
            //    //                // notify all the threads to stop; avoid deadlocks by stopping any
            //    //                // further state reports from the thread since we're shutting down
            //    //        foreach (StreamThread thread in threads)
            //    //        {
            //    //            thread.shutdown();
            //    //        }

            //    //        foreach (StreamThread thread in threads)
            //    //        {
            //    //            try
            //    //            {
            //    //                if (!thread.isRunning())
            //    //                {
            //    //                    thread.join();
            //    //                }
            //    //            }
            //    //            catch (Exception ex)
            //    //            {
            //    //                Thread.CurrentThread.Interrupt();
            //    //            }
            //    //        }

            //    //        if (globalStreamThread != null)
            //    //        {
            //    //            globalStreamThread.shutdown();
            //    //        }

            //    //        if (globalStreamThread != null && !globalStreamThread.stillRunning())
            //    //        {
            //    //            try
            //    //            {
            //    //                globalStreamThread.join();
            //    //            }
            //    //            catch (Exception e)
            //    //            {
            //    //                Thread.CurrentThread.Interrupt();
            //    //            }

            //    //            globalStreamThread = null;
            //    //        }

            //    //        adminClient.close();

            //    //        metrics.close();
            //    //        setState(State.NOT_RUNNING);
            //    //    }, "kafka-streams-close-thread");

            //    //    shutdownThread.setDaemon(true);
            //    //    shutdownThread.start();
            //    //}

            //    if (waitOnState(State.NOT_RUNNING, timeoutMs))
            //    {
            //        log.LogInformation("Streams client stopped completely");
            //        return true;
            //    }
            //    else
            //    {
            //        log.LogInformation("Streams client cannot stop completely within the timeout");
            //        return false;
            //    }
            //}
        }

        /**
         * Shutdown this {@code KafkaStreams} by signaling all the threads to stop, and then wait up to the timeout for the
         * threads to join.
         * A {@code timeout} of 0 means to wait forever.
         *
         * @param timeout  how long to wait for the threads to shutdown
         * @return {@code true} if all threads were successfully stopped&mdash;{@code false} if the timeout was reached
         * before all threads stopped
         * Note that this method must not be called in the {@link StateListener#onChange(KafkaStreams.State, KafkaStreams.State)} callback of {@link StateListener}.
         * @throws ArgumentException if {@code timeout} can't be represented as {@code long milliseconds}
         */
        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool close(TimeSpan timeout)
        {
            string msgPrefix = ""; // prepareMillisCheckFailMsgPrefix(timeout, "timeout");
            var timeoutMs = ApiUtils.validateMillisecondDuration(timeout, msgPrefix);

            if (timeoutMs < TimeSpan.Zero)
            {
                throw new ArgumentException("Timeout can't be negative.");
            }

            log.LogDebug("Stopping Streams client with timeoutMillis = {} ms.", timeoutMs);

            return close(timeoutMs);
        }

        /**
         * Do a clean up of the local {@link StateStore} directory ({@link StreamsConfig#STATE_DIR_CONFIG}) by deleting all
         * data with regard to the {@link StreamsConfig#APPLICATION_ID_CONFIG application ID}.
         * <p>
         * May only be called either before this {@code KafkaStreams} instance is {@link #start() started} or after the
         * instance is {@link #close() closed}.
         * <p>
         * Calling this method triggers a restore of local {@link StateStore}s on the next {@link #start() application start}.
         *
         * @throws IllegalStateException if this {@code KafkaStreams} instance is currently {@link State#RUNNING running}
         * @throws StreamsException if cleanup failed
         */
        public void cleanUp()
        {
            if (isRunning())
            {
                throw new Exception("Cannot clean up while running.");
            }

            //stateDirectory.clean();
        }

        /**
         * Find all currently running {@code KafkaStreams} instances (potentially remotely) that use the same
         * {@link StreamsConfig#APPLICATION_ID_CONFIG application ID} as this instance (i.e., all instances that belong to
         * the same Kafka Streams application) and return {@link StreamsMetadata} for each discovered instance.
         * <p>
         * Note: this is a point in time view and it may change due to partition reassignment.
         *
         * @return {@link StreamsMetadata} for each {@code KafkaStreams} instances of this application
         */
        public HashSet<StreamsMetadata> allMetadata()
        {
            validateIsRunning();
            return new HashSet<StreamsMetadata>(streamsMetadataState.getAllMetadata());
        }

        /**
         * Find all currently running {@code KafkaStreams} instances (potentially remotely) that
         * <ul>
         *   <li>use the same {@link StreamsConfig#APPLICATION_ID_CONFIG application ID} as this instance (i.e., all
         *       instances that belong to the same Kafka Streams application)</li>
         *   <li>and that contain a {@link StateStore} with the given {@code storeName}</li>
         * </ul>
         * and return {@link StreamsMetadata} for each discovered instance.
         * <p>
         * Note: this is a point in time view and it may change due to partition reassignment.
         *
         * @param storeName the {@code storeName} to find metadata for
         * @return {@link StreamsMetadata} for each {@code KafkaStreams} instances with the provide {@code storeName} of
         * this application
         */
        public HashSet<StreamsMetadata> allMetadataForStore(string storeName)
        {
            validateIsRunning();
            return new HashSet<StreamsMetadata>(streamsMetadataState.getAllMetadataForStore(storeName));
        }

        /**
         * Find the currently running {@code KafkaStreams} instance (potentially remotely) that
         * <ul>
         *   <li>use the same {@link StreamsConfig#APPLICATION_ID_CONFIG application ID} as this instance (i.e., all
         *       instances that belong to the same Kafka Streams application)</li>
         *   <li>and that contain a {@link StateStore} with the given {@code storeName}</li>
         *   <li>and the {@link StateStore} Contains the given {@code key}</li>
         * </ul>
         * and return {@link StreamsMetadata} for it.
         * <p>
         * This will use the default Kafka Streams partitioner to locate the partition.
         * If a {@link StreamPartitioner custom partitioner} has been
         * {@link ProducerConfig#PARTITIONER_CLASS_CONFIG configured} via {@link StreamsConfig} or
         * {@link KStream#through(String, Produced)}, or if the original {@link KTable}'s input
         * {@link StreamsBuilder#table(String) topic} is partitioned differently, please use
         * {@link #metadataForKey(String, Object, StreamPartitioner)}.
         * <p>
         * Note:
         * <ul>
         *   <li>this is a point in time view and it may change due to partition reassignment</li>
         *   <li>the key may not exist in the {@link StateStore}; this method provides a way of finding which host it
         *       <em>would</em> exist on</li>
         *   <li>if this is for a window store the serializer should be the serializer for the record key,
         *       not the window serializer</li>
         * </ul>
         *
         * @param storeName     the {@code storeName} to find metadata for
         * @param key           the key to find metadata for
         * @param keySerializer serializer for the key
         * @param <K>           key type
         * @return {@link StreamsMetadata} for the {@code KafkaStreams} instance with the provide {@code storeName} and
         * {@code key} of this application or {@link StreamsMetadata#NOT_AVAILABLE} if Kafka Streams is (re-)initializing
         */
        public StreamsMetadata metadataForKey<K>(
            string storeName,
            K key,
            ISerializer<K> keySerializer)
        {
            validateIsRunning();
            return null;// streamsMetadataState.getMetadataWithKey(storeName, key, keySerializer);
        }

        /**
         * Find the currently running {@code KafkaStreams} instance (potentially remotely) that
         * <ul>
         *   <li>use the same {@link StreamsConfig#APPLICATION_ID_CONFIG application ID} as this instance (i.e., all
         *       instances that belong to the same Kafka Streams application)</li>
         *   <li>and that contain a {@link StateStore} with the given {@code storeName}</li>
         *   <li>and the {@link StateStore} Contains the given {@code key}</li>
         * </ul>
         * and return {@link StreamsMetadata} for it.
         * <p>
         * Note:
         * <ul>
         *   <li>this is a point in time view and it may change due to partition reassignment</li>
         *   <li>the key may not exist in the {@link StateStore}; this method provides a way of finding which host it
         *       <em>would</em> exist on</li>
         * </ul>
         *
         * @param storeName   the {@code storeName} to find metadata for
         * @param key         the key to find metadata for
         * @param partitioner the partitioner to be use to locate the host for the key
         * @param <K>         key type
         * @return {@link StreamsMetadata} for the {@code KafkaStreams} instance with the provide {@code storeName} and
         * {@code key} of this application or {@link StreamsMetadata#NOT_AVAILABLE} if Kafka Streams is (re-)initializing
         */
        public StreamsMetadata metadataForKey<K, V>(
            string storeName,
            K key,
            IStreamPartitioner<K, V> partitioner)
        {
            validateIsRunning();
            return null; // streamsMetadataState.getMetadataWithKey(storeName, key, partitioner);
        }

        /**
         * Get a facade wrapping the local {@link StateStore} instances with the provided {@code storeName} if the Store's
         * type is accepted by the provided {@link QueryableStoreType#accepts(StateStore) queryableStoreType}.
         * The returned object can be used to query the {@link StateStore} instances.
         *
         * @param storeName           name of the store to find
         * @param queryableStoreType  accept only stores that are accepted by {@link QueryableStoreType#accepts(StateStore)}
         * @param <T>                 return type
         * @return A facade wrapping the local {@link StateStore} instances
         * @throws InvalidStateStoreException if Kafka Streams is (re-)initializing or a store with {@code storeName} and
         * {@code queryableStoreType} doesn't exist
         */
        //public T store<T>(string storeName, IQueryableStoreType<T> queryableStoreType)
        //{
        //    validateIsRunning();
        //    return queryableStoreProvider.getStore(storeName, queryableStoreType);
        //}

        /**
         * Returns runtime information about the local threads of this {@link KafkaStreams} instance.
         *
         * @return the set of {@link ThreadMetadata}.
         */
        //public HashSet<ThreadMetadata> localThreadsMetadata()
        //{
        //    validateIsRunning();
        //    HashSet<ThreadMetadata> threadMetadata = new HashSet<ThreadMetadata>();
        //    foreach (StreamThread thread in threads)
        //    {
        //        //  threadMetadata.Add(thread.threadMetadata());
        //    }

        //    return threadMetadata;
        //}

        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~KafkaStreams()
        // {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
    }
}