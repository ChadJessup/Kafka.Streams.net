using Confluent.Kafka;
using Kafka.Common;
using Kafka.Common.Utils;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Clients;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Internals;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Threads.GlobalStream;
using Kafka.Streams.Threads.KafkaStream;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Kafka.Streams.Threads.KafkaStreams
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
     * props.Add(StreamsConfig.BootstrapServers, "localhost:9092");
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
    public class KafkaStreamsThread : IKafkaStreamsThread
    {
        private readonly ILogger<KafkaStreamsThread> logger;
        private readonly IDisposable logContext;

        private readonly ITime time;
        private readonly Guid processId;
        private readonly IServiceProvider services;
        private readonly IKafkaClientSupplier clientSupplier;
        private readonly StreamsConfig config;

        protected IKafkaStreamThread[] threads;

        private StateDirectory stateDirectory;
        private StreamsMetadataState streamsMetadataState;
        // private ScheduledExecutorService stateDirCleaner;
        private QueryableStoreProvider queryableStoreProvider;
        private IAdminClient adminClient;

        private IStateRestoreListener globalStateRestoreListener;
        private readonly IGlobalStreamThread globalStreamThread;
        private readonly Topology topology;
        private readonly object stateLock = new object();

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
        public KafkaStreamsThread(
            ILogger<KafkaStreamsThread> logger,
            IServiceProvider serviceProvider,
            IGlobalStreamThread globalStreamThread,
            IStateMachine<KafkaStreamsThreadStates> states,
            StreamsConfig config,
            Topology topology,
            IKafkaClientSupplier clientSupplier)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.topology = topology ?? throw new ArgumentNullException(nameof(topology));
            this.clientSupplier = clientSupplier ?? throw new ArgumentNullException(nameof(clientSupplier));
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            this.State = states ?? throw new ArgumentNullException(nameof(states));

            this.logContext = this.logger.BeginScope($"stream-client [{config.ClientId}] ");

            this.globalStreamThread = globalStreamThread;

            this.services = serviceProvider;
            this.time = Time.SYSTEM;

            this.processId = Guid.NewGuid();

            // The application ID is a required config and hence should always have value
            string applicationId = config.ApplicationId;
            config.ClientId ??= $"{applicationId}-{processId}";
            
            this.adminClient = this.clientSupplier.GetAdminClient(this.config.GetAdminConfigs(StreamsBuilder.GetSharedAdminClientId(config.ClientId)));

            // re-write the physical topology according to the config
            topology.internalTopologyBuilder.RewriteTopology(config);

            // sanity check to fail-fast in case we cannot build a ProcessorTopology due to an exception
            var taskTopology = topology.internalTopologyBuilder.build();

            //create the stream thread, global update thread, and cleanup thread
            threads = new KafkaStreamThread[config.NumberOfStreamThreads];

            long totalCacheSize = config.CacheMaxBytesBuffering;

            if (totalCacheSize < 0)
            {
                totalCacheSize = 0;
                logger.LogWarning("Negative cache size passed in. Reverting to cache size of 0 bytes.");
            }

            var globalTaskTopology = topology.internalTopologyBuilder.buildGlobalStateTopology();
            
            long cacheSizePerThread = totalCacheSize / (threads.Length + (globalTaskTopology == null ? 0 : 1));

            bool createStateDirectory = taskTopology.hasPersistentLocalStore()
                || (globalTaskTopology != null && globalTaskTopology.hasPersistentGlobalStore());

            GlobalStreamThreadState? globalThreadState = null;

            if (globalTaskTopology != null)
            {
                string globalThreadId = $"{config.ClientId}-GlobalStreamThread";

                //globalStreamThread = ActivatorUtilities.CreateInstance<GlobalStreamThread>(
                //    this.services,
                //    globalTaskTopology,
                //    config,
                //    clientSupplier.getGlobalConsumer(config.GetGlobalConsumerConfigs(clientId)),
                //    stateDirectory,
                //    cacheSizePerThread,
                //    time,
                //    globalThreadId,
                //    delegatingStateRestoreListener);

                globalThreadState = globalStreamThread.State as GlobalStreamThreadState
                    ?? throw new ArgumentException($"Expected a {nameof(GlobalStreamThreadState)} got {globalStreamThread.State.GetType()}");
            }

            var storeProviders = new List<IStateStoreProvider>();
            for (int i = 0; i < threads.Length; i++)
            {
                threads[i] = ActivatorUtilities.GetServiceOrCreateInstance<IKafkaStreamThread>(this.services);

                this.ThreadStates.Add(threads[i].ManagedThreadId, threads[i].State as KafkaStreamThreadState);
                storeProviders.Add(new StreamThreadStateStoreProvider(threads[i]));
            }

            var globalStateStoreProvider = new GlobalStateStoreProvider(topology.internalTopologyBuilder.globalStateStores());
            queryableStoreProvider = new QueryableStoreProvider(storeProviders, globalStateStoreProvider);

            //stateDirCleaner = Executors.newSingleThreadScheduledExecutor(r =>
            //{
            //    Thread thread = new Thread(r, clientId + "-CleanupThread");
            //    thread.setDaemon(true);
            //    return thread;
            //});
        }

        public Dictionary<long, KafkaStreamThreadState> ThreadStates { get; } = new Dictionary<long, KafkaStreamThreadState>();

        private bool WaitOnState(KafkaStreamsThreadStates targetState, long waitMs)
        {
            long begin = time.milliseconds();
            lock (stateLock)
            {
                long elapsedMs = 0L;
                while (this.State.CurrentState != targetState)
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
                        this.logger.LogDebug($"Cannot transit to {targetState} within {waitMs}ms");

                        return false;
                    }

                    elapsedMs = time.milliseconds() - begin;
                }

                return true;
            }
        }

        public IStateListener GetStateListener()
        {
            if (this.StateListener != null)
            {
                return this.StateListener;
            }

            IStateListener streamStateListener = ActivatorUtilities.GetServiceOrCreateInstance<IStateListener>(this.services);

            streamStateListener.SetThreadStates(this.ThreadStates);

            return streamStateListener;
        }


        public bool IsRunning()
        {
            lock (stateLock)
            {
                return this.State.IsRunning();
            }
        }

        private void ValidateIsRunning()
        {
            if (!IsRunning())
            {
                throw new Exception($"KafkaStreams is not running. State is {this.State}.");
            }
        }

        /**
         * An app can set a single {@link KafkaStreams.StateListener} so that the app is notified when state changes.
         *
         * @param listener a new state listener
         * @throws Exception if this {@code KafkaStreams} instance is not in state {@link State#CREATED CREATED}.
         */
        public void SetStateListener(IStateListener listener)
        {
            lock (stateLock)
            {
                if (this.State.CurrentState == KafkaStreamsThreadStates.CREATED)
                {
                    this.StateListener = listener;
                }
                else
                {
                    throw new Exception($"Can only set StateListener in CREATED state. Current state is: {this.State}");
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
        public void SetUncaughtExceptionHandler(/*UncaughtExceptionHandler eh*/)
        {
            lock (stateLock)
            {
                if (this.State.CurrentState == KafkaStreamsThreadStates.CREATED)
                {
                    foreach (var thread in threads)
                    {
                        //context.Thread.setUncaughtExceptionHandler(eh);
                    }

                    if (this.globalStreamThread != null)
                    {
                        // globalStreamThread.setUncaughtExceptionHandler(eh);
                    }
                }
                else
                {
                    throw new Exception("Can only set UncaughtExceptionHandler in CREATED state. " +
                        "Current state is: " + this.State);
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
        public void SetGlobalStateRestoreListener(IStateRestoreListener globalStateRestoreListener)
        {
            lock (stateLock)
            {
                if (this.State.CurrentState == KafkaStreamsThreadStates.CREATED)
                {
                    this.globalStateRestoreListener = globalStateRestoreListener;
                }
                else
                {
                    throw new Exception("Can only set GlobalStateRestoreListener in CREATED state. " +
                        "Current state is: " + this.State);
                }
            }
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
        public void Start()
        {
            if (this.State.SetState(KafkaStreamsThreadStates.REBALANCING))
            {
                this.logger.LogInformation("Starting Streams client");

                if (globalStreamThread != null)
                {
                    globalStreamThread.Start();
                }

                foreach (var thread in threads)
                {
                    thread.Start();
                }

                long cleanupDelay = this.config.getLong(StreamsConfigPropertyNames.STATE_CLEANUP_DELAY_MS_CONFIG).Value;

                //stateDirCleaner.scheduleAtFixedRate(()=> {
                //    // we do not use lock here since we only read on the value and act on it
                //    if (state == State.RUNNING)
                //    {
                //        stateDirectory.cleanRemovedTasks(cleanupDelay);
                //    }
                //}, cleanupDelay, cleanupDelay, TimeUnit.MILLISECONDS);
            }
            else
            {
                throw new Exception("The client is either already started or already stopped, cannot re-start");
            }
        }

        /**
         * Shutdown this {@code KafkaStreams} instance by signaling all the threads to stop, and then wait for them to join.
         * This will block until all threads have stopped.
         */
        public void Close()
        {
            Close(long.MaxValue);
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
        public bool Close(long timeout, TimeUnit timeUnit)
        {
            long timeoutMs = 0; // timeUnit.toMillis(timeout);

            logger.LogDebug($"Stopping Streams client with timeoutMillis = {timeoutMs} ms. You are using deprecated method. " +
                "Please, consider update your code.");

            if (timeoutMs < 0)
            {
                timeoutMs = 0;
            }
            else if (timeoutMs == 0)
            {
                timeoutMs = long.MaxValue;
            }

            return Close(timeoutMs);
        }

        private bool Close(long timeoutMs)
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
            //    //        foreach (KafkaStreamThread thread in threads)
            //    //        {
            //    //            thread.shutdown();
            //    //        }

            //    //        foreach (KafkaStreamThread thread in threads)
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
        public bool Close(TimeSpan timeout)
        {
            string msgPrefix = ApiUtils.prepareMillisCheckFailMsgPrefix(timeout, "timeout");
            var timeoutMs = ApiUtils.validateMillisecondDuration(timeout, msgPrefix);

            if (timeoutMs < TimeSpan.Zero)
            {
                throw new ArgumentException("Timeout can't be negative.");
            }

            logger.LogDebug($"Stopping Streams client with timeoutMillis = {timeoutMs} ms.");

            return Close(timeoutMs);
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
        public void CleanUp()
        {
            if (IsRunning())
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
        public HashSet<StreamsMetadata> AllMetadata()
        {
            ValidateIsRunning();
            return new HashSet<StreamsMetadata>(streamsMetadataState.GetAllMetadata());
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
        public HashSet<StreamsMetadata> AllMetadataForStore(string storeName)
        {
            ValidateIsRunning();
            return new HashSet<StreamsMetadata>(streamsMetadataState.GetAllMetadataForStore(storeName));
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
        public StreamsMetadata MetadataForKey<K>(
            string storeName,
            K key,
            ISerializer<K> keySerializer)
        {
            ValidateIsRunning();
            return streamsMetadataState.GetMetadataWithKey(storeName, key, keySerializer);
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
        public StreamsMetadata MetadataForKey<K, V>(
            string storeName,
            K key,
            IStreamPartitioner<K, V> partitioner)
        {
            ValidateIsRunning();
            return streamsMetadataState.GetMetadataWithKey(storeName, key, partitioner);
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
        //    foreach (KafkaStreamThread thread in threads)
        //    {
        //        //  threadMetadata.Add(thread.threadMetadata());
        //    }

        //    return threadMetadata;
        //}

        private bool disposedValue = false; // To detect redundant calls

        public string ThreadClientId { get; }
        public Thread Thread { get; }
        public int ManagedThreadId { get; }
        public IStateListener StateListener { get; private set; }
        public IStateMachine<KafkaStreamsThreadStates> State { get; }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                    this.adminClient?.Dispose();
                    this.logContext?.Dispose();
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