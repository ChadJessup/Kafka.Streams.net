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
using Confluent.Kafka;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Errors;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals.Metrics;
using Kafka.Streams.State.Internals;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
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
        private StreamsException startupException;


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
                IGlobalStateManager stateMgr = new GlobalStateManagerImpl(
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
            return Collections.unmodifiableMap(globalConsumer.metrics);
        }

        internal void setStateListener(StreamStateListener streamStateListener)
        {
            throw new NotImplementedException();
        }
    }
}