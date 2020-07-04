using Kafka.Common;
using Kafka.Streams.Clients;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Kafka.Streams.Threads.GlobalStream
{
    public class GlobalStreamThread : IGlobalStreamThread
    {
        private readonly ILogger<GlobalStreamThread> logger;
        private readonly IDisposable logPrefix;
        private readonly StreamsConfig config;
        private readonly IKafkaClientSupplier clientSupplier;
        private readonly ILoggerFactory loggerFactory;
        private readonly IStateRestoreListener stateRestoreListener;
        private readonly GlobalConsumer globalConsumer;
        private readonly IClock clock;
        private readonly StateDirectory stateDirectory;
        // private ThreadCache cache;
        private readonly ProcessorTopology topology;
        private StreamsException startupException;

        public Thread Thread { get; }
        public void Join() => this.Thread?.Join();
        public IThreadStateMachine<GlobalStreamThreadStates> State { get; }
        public IStateListener StateListener { get; private set; }
        public string ThreadClientId { get; }

        public int ManagedThreadId => this.Thread.ManagedThreadId;

        public GlobalStreamThread(
            ILogger<GlobalStreamThread> logger,
            ILoggerFactory loggerFactory,
            StreamsConfig config,
            IThreadStateMachine<GlobalStreamThreadStates> states,
            IKafkaClientSupplier clientSupplier,
            StateDirectory stateDirectory,
            Topology topology)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.State = states ?? throw new ArgumentNullException(nameof(states));
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            this.clientSupplier = clientSupplier ?? throw new ArgumentNullException(nameof(clientSupplier));
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            this.stateDirectory = stateDirectory ?? throw new ArgumentNullException(nameof(stateDirectory));

            var topologyBuilder = topology?.internalTopologyBuilder?.SetApplicationId(config.ApplicationId) ?? throw new ArgumentNullException(nameof(topology));
            this.topology = topologyBuilder.BuildGlobalStateTopology();

            this.ThreadClientId = $"{config.ClientId}-GlobalStreamThread";
            this.logPrefix = this.logger.BeginScope($"global-stream-thread [{this.ThreadClientId}] ");
            this.globalConsumer = this.clientSupplier.GetGlobalConsumer();

            this.Thread = new Thread(this.Run);

            //this.time = time;
            //this.cache = new ThreadCache(logContext, cacheSizeBytes, streamsMetrics);
        }

        public void Run()
        {
            using StateConsumer? stateConsumer = this.Initialize();

            if (stateConsumer == null)
            {
                // during initialization, the caller thread would wait for the state consumer
                // to restore the global state store before transiting to RUNNING state and return;
                // if an error happens during the restoration process, the stateConsumer will be null
                // and in this case we will transit the state to PENDING_SHUTDOWN and DEAD immediately.
                // the exception will be thrown in the caller thread during start() function.
                this.State.SetState(GlobalStreamThreadStates.PENDING_SHUTDOWN);
                this.State.SetState(GlobalStreamThreadStates.DEAD);

                this.logger.LogWarning("Error happened during initialization of the global state store; this thread has shutdown");

                return;
            }

            this.State.SetState(GlobalStreamThreadStates.RUNNING);

            try
            {
                while (this.IsRunning())
                {
                    stateConsumer.PollAndUpdate();
                }
            }
            finally
            {
                // set the state to pending shutdown first as it may be called due to error;
                // its state may already be PENDING_SHUTDOWN so it will return false but we
                // intentionally do not check the returned flag
                this.State.SetState(GlobalStreamThreadStates.PENDING_SHUTDOWN);

                this.logger.LogInformation("Shutting down");

                try
                {
                    stateConsumer?.Close();
                }
                catch (IOException e)
                {
                    this.logger.LogError("Failed to Close state maintainer due to the following error:", e);
                }

                this.State.SetState(GlobalStreamThreadStates.DEAD);

                this.logger.LogInformation("Shutdown complete");
            }
        }

        private StateConsumer? Initialize()
        {
            try
            {
                IGlobalStateManager stateMgr = new GlobalStateManager(
                    this.loggerFactory.CreateLogger<GlobalStateManager>(),
                    this.topology,
                    this.clientSupplier,
                    this.globalConsumer,
                    this.stateDirectory,
                    this.stateRestoreListener,
                    this.config);

                //GlobalProcessorContextImpl globalProcessorContext = new GlobalProcessorContextImpl(
                //    config,
                //    stateMgr,
                //    streamsMetrics,
                //    cache);

                //stateMgr.setGlobalProcessorContext(globalProcessorContext);

                var stateConsumer = new StateConsumer(
                    null,
                    this.globalConsumer,
                    null,
                    //new GlobalStateUpdateTask(
                    //    topology,
                    //    globalProcessorContext,
                    //    stateMgr,
                    //    config.defaultDeserializationExceptionHandler(),
                    //    logContext
                    //),
                    this.clock,
                    TimeSpan.FromMilliseconds((double)this.config.PollMs),
                    this.config.CommitIntervalMs);

                stateConsumer.Initialize();

                return stateConsumer;
            }
            catch (LockException fatalException)
            {
                var errorMsg = "Could not lock global state directory. This could happen if multiple KafkaStreams " +
                    "instances are running on the same host using the same state directory.";

                this.logger.LogError(errorMsg, fatalException);

                this.startupException = new StreamsException(errorMsg, fatalException);
            }
            catch (StreamsException fatalException)
            {
                this.startupException = fatalException;
            }
            catch(ArgumentNullException argumentNullException)
            {
                this.logger.LogError(argumentNullException, $"Null value tried to be used when creating {nameof(StateConsumer)} for {nameof(GlobalStreamThread)}");
            }
            catch (Exception fatalException)
            {
                this.startupException = new StreamsException("Exception caught during initialization of GlobalStreamThread", fatalException);
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Start()
        {
            this.Thread.Start();

            while (!this.IsRunning())
            {
                Thread.Sleep(1);
                if (this.startupException != null)
                {
                    throw this.startupException;
                }
            }
        }

        public void Shutdown()
        {
            // one could call shutdown() multiple times, so ignore subsequent calls
            // if already shutting down or dead
            this.State.SetState(GlobalStreamThreadStates.PENDING_SHUTDOWN);
        }

        public void SetStateListener(IStateListener listener)
            => this.StateListener = listener;

        public bool IsRunning()
            => this.State.IsRunning();

        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                this.disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~GlobalStreamThread()
        // {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            this.Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
    }
}