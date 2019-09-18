using Confluent.Kafka;
using Kafka.Common.Metrics;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Consumer;
using Kafka.Streams.Consumers;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals.Metrics;
using Kafka.Streams.Processors;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Kafka.Streams.Processor.Internals
{
    public class GlobalStreamThread : IThread<GlobalStreamThreadStates>
    {
        private readonly ILogger<GlobalStreamThread> logger;

        public IStateMachine<GlobalStreamThreadStates> State { get; }

        private readonly LogContext logContext;
        private readonly IStateRestoreListener stateRestoreListener;
        private readonly StreamsConfig config;
        private readonly IConsumer<byte[], byte[]> globalConsumer;
        private readonly ITime time;
        private readonly StateDirectory stateDirectory;
        // private ThreadCache cache;
        private readonly StreamsMetricsImpl streamsMetrics;
        private readonly string logPrefix;
        private readonly ProcessorTopology topology;
        private StreamsException startupException;

        public Thread Thread { get; }
        public IStateListener StateListener { get; private set; }
        public string ThreadClientId { get; }

        public GlobalStreamThread(
            ILogger<GlobalStreamThread> logger,
            GlobalStreamThreadState globalStreamThreadState,
            ProcessorTopology topology,
            StreamsConfig config,
            GlobalConsumer globalConsumer,
            StateDirectory stateDirectory,
            long cacheSizeBytes,
            MetricsRegistry metrics,
            ITime time,
            string threadClientId,
            IStateRestoreListener stateRestoreListener)
        {
            this.logger = logger;
            this.State = globalStreamThreadState;

            this.ThreadClientId = threadClientId;
            this.time = time;
            this.config = config;
            this.topology = topology;
            this.globalConsumer = globalConsumer;
            this.stateDirectory = stateDirectory;
            this.streamsMetrics = new StreamsMetricsImpl(metrics, threadClientId);
            this.logPrefix = string.Format("global-stream-thread [%s] ", threadClientId);
            this.logContext = new LogContext(logPrefix);
            // this.cache = new ThreadCache(logContext, cacheSizeBytes, streamsMetrics);
            this.stateRestoreListener = stateRestoreListener;
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
                this.State.setState(GlobalStreamThreadStates.PENDING_SHUTDOWN);
                this.State.setState(GlobalStreamThreadStates.DEAD);

                logger.LogWarning("Error happened during initialization of the global state store; this thread has shutdown");
                streamsMetrics.removeAllThreadLevelSensors();

                return;
            }

            this.State.setState(GlobalStreamThreadStates.RUNNING);

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
                this.State.setState(GlobalStreamThreadStates.PENDING_SHUTDOWN);

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

                this.State.setState(GlobalStreamThreadStates.DEAD);

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

                StateConsumer stateConsumer = new StateConsumer(
                    null,
                    globalConsumer,
                    null,
                    //new GlobalStateUpdateTask(
                    //    topology,
                    //    globalProcessorContext,
                    //    stateMgr,
                    //    config.defaultDeserializationExceptionHandler(),
                    //    logContext
                    //),
                    time,
                    TimeSpan.FromMilliseconds((double)config.getLong(StreamsConfigPropertyNames.POLL_MS_CONFIG)),
                    config.getLong(StreamsConfigPropertyNames.COMMIT_INTERVAL_MS_CONFIG).Value
                );

                stateConsumer.initialize();

                return stateConsumer;
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
        public void start()
        {
            this.Thread.Start();

            while (!stillRunning())
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
            this.State.setState(GlobalStreamThreadStates.PENDING_SHUTDOWN);
        }

        public void setStateListener(IStateListener listener)
            => this.StateListener = listener;

        public bool stillRunning()
            => this.State.isRunning();
    }
}