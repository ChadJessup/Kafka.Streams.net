using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Processors.Internals.Metrics;
using Kafka.Streams.State;
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

        private readonly IStateRestoreListener stateRestoreListener;
        private readonly GlobalConsumer globalConsumer;
        private readonly ITime time;
        private readonly StateDirectory stateDirectory;
        // private ThreadCache cache;
        private readonly StreamsMetricsImpl streamsMetrics;
        private readonly string logPrefix;
        private readonly ProcessorTopology topology;
        private StreamsException startupException;

        public Thread Thread { get; }
        public IStateMachine<GlobalStreamThreadStates> State { get; }
        public IStateListener StateListener { get; private set; }
        public string ThreadClientId { get; }
        public IThreadContext<IThread<GlobalStreamThreadStates>, IStateMachine<GlobalStreamThreadStates>, GlobalStreamThreadStates> Context { get; private set; }

        public int ManagedThreadId => this.Thread.ManagedThreadId;

        public GlobalStreamThread(ILogger<GlobalStreamThread> logger)
        {
            this.logger = logger;

            this.Thread = new Thread(run);

            //this.ThreadClientId = threadClientId;
            //this.time = time;
            //this.config = config;
            //this.topology = topology;
            //this.globalConsumer = globalConsumer;
            //this.stateDirectory = stateDirectory;
            //this.streamsMetrics = new StreamsMetricsImpl(metrics, threadClientId);
            //this.logPrefix = $"global-stream-thread [{threadClientId}] ";
            //this.logContext = new LogContext(logPrefix);
            // this.cache = new ThreadCache(logContext, cacheSizeBytes, streamsMetrics);
            //this.stateRestoreListener = stateRestoreListener;
        }
        public void SetContext(IThreadContext<IThread<GlobalStreamThreadStates>, IStateMachine<GlobalStreamThreadStates>, GlobalStreamThreadStates> context)
            => this.Context = context ?? throw new ArgumentNullException(nameof(context));

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
                this.Context.State.SetState(GlobalStreamThreadStates.PENDING_SHUTDOWN);
                this.Context.State.SetState(GlobalStreamThreadStates.DEAD);

                logger.LogWarning("Error happened during initialization of the global state store; this thread has shutdown");
                streamsMetrics.removeAllThreadLevelSensors();

                return;
            }

            this.Context.State.SetState(GlobalStreamThreadStates.RUNNING);

            try
            {
                while (IsRunning())
                {
                    stateConsumer.pollAndUpdate();
                }
            }
            finally
            {

                // set the state to pending shutdown first as it may be called due to error;
                // its state may already be PENDING_SHUTDOWN so it will return false but we
                // intentionally do not check the returned flag
                this.Context.State.SetState(GlobalStreamThreadStates.PENDING_SHUTDOWN);

                logger.LogInformation("Shutting down");

                try
                {
                    stateConsumer.close();
                }
                catch (IOException e)
                {
                    logger.LogError("Failed to close state maintainer due to the following error:", e);
                }

                streamsMetrics.removeAllThreadLevelSensors();

                this.Context.State.SetState(GlobalStreamThreadStates.DEAD);

                logger.LogInformation("Shutdown complete");
            }
        }

        private StateConsumer initialize()
        {
            try
            {
                //IGlobalStateManager stateMgr = new GlobalStateManagerImpl(
                //    logContext,
                //    topology,
                //    globalConsumer,
                //    stateDirectory,
                //    stateRestoreListener,
                //    config);

                //GlobalProcessorContextImpl globalProcessorContext = new GlobalProcessorContextImpl(
                //    config,
                //    stateMgr,
                //    streamsMetrics,
                //    cache);

                //stateMgr.setGlobalProcessorContext(globalProcessorContext);

                //StateConsumer stateConsumer = new StateConsumer(
                //    null,
                //    globalConsumer,
                //    null,
                //    //new GlobalStateUpdateTask(
                //    //    topology,
                //    //    globalProcessorContext,
                //    //    stateMgr,
                //    //    config.defaultDeserializationExceptionHandler(),
                //    //    logContext
                //    //),
                //    time,
                //    TimeSpan.FromMilliseconds((double)config.getLong(StreamsConfigPropertyNames.PollMs)),
                //    config.getLong(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG).Value
                //);

                // stateConsumer.initialize();

                return null; // stateConsumer;
            }
            catch (LockException fatalException)
            {
                string errorMsg = "Could not lock global state directory. This could happen if multiple KafkaStreams " +
                    "instances are running on the same host using the same state directory.";

                this.logger.LogError(errorMsg, fatalException);

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
        public void Start()
        {
            this.Thread.Start();

            while (!IsRunning())
            {
                Thread.Sleep(1);
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
            this.Context.State.SetState(GlobalStreamThreadStates.PENDING_SHUTDOWN);
        }

        public void setStateListener(IStateListener listener)
            => this.StateListener = listener;

        public bool IsRunning()
            => this.Context.State.IsRunning();

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
        // ~GlobalStreamThread()
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