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
using Kafka.Streams.IProcessor.Interfaces;

namespace Kafka.Streams.IProcessor.Internals
{
    public abstract class AbstractProcessorContext : IInternalProcessorContext
    {


        public static string NONEXIST_TOPIC = "__null_topic__";
        private TaskId taskId;
        private string applicationId;
        private StreamsConfig config;
        private StreamsMetricsImpl metrics;
        private Serde keySerde;
        private ThreadCache cache;
        private Serde valueSerde;
        private bool initialized;
        protected ProcessorRecordContext recordContext;
        protected ProcessorNode currentNode;
        IStateManager stateManager;

        public AbstractProcessorContext(TaskId taskId,
                                        StreamsConfig config,
                                        StreamsMetricsImpl metrics,
                                        IStateManager stateManager,
                                        ThreadCache cache)
        {
            this.taskId = taskId;
            this.applicationId = config.getString(StreamsConfig.APPLICATION_ID_CONFIG);
            this.config = config;
            this.metrics = metrics;
            this.stateManager = stateManager;
            valueSerde = config.defaultValueSerde();
            keySerde = config.defaultKeySerde();
            this.cache = cache;
        }


        public string applicationId()
        {
            return applicationId;
        }


        public TaskId taskId()
        {
            return taskId;
        }


        public ISerde<object> keySerde
        {
        return keySerde;
        }


        public ISerde<object> valueSerde
        {
        return valueSerde;
        }


        public FileInfo stateDir()
        {
            return stateManager.baseDir();
        }


        public StreamsMetricsImpl metrics()
        {
            return metrics;
        }


        public void register(IStateStore store,
                             IStateRestoreCallback stateRestoreCallback)
        {
            if (initialized)
            {
                throw new InvalidOperationException("Can only create state stores during initialization.");
            }
            store = store ?? throw new System.ArgumentNullException("store must not be null", nameof(store));
            stateManager.register(store, stateRestoreCallback);
        }

        /**
         * @throws InvalidOperationException if the task's record is null
         */

        public string Topic
        {
            get
            {
                if (recordContext == null)
                {
                    throw new InvalidOperationException("This should not happen as Topic should only be called while a record is processed");
                }

                string topic = recordContext.Topic;

                if (topic.Equals(NONEXIST_TOPIC))
                {
                    return null;
                }

                return topic;
            }
        }

        /**
         * @throws InvalidOperationException if partition is null
         */

        public int partition()
        {
            if (recordContext == null)
            {
                throw new InvalidOperationException("This should not happen as partition() should only be called while a record is processed");
            }
            return recordContext.partition();
        }

        /**
         * @throws InvalidOperationException if offset is null
         */

        public long offset()
        {
            if (recordContext == null)
            {
                throw new InvalidOperationException("This should not happen as offset() should only be called while a record is processed");
            }
            return recordContext.offset();
        }


        public Headers headers()
        {
            if (recordContext == null)
            {
                throw new InvalidOperationException("This should not happen as headers() should only be called while a record is processed");
            }
            return recordContext.headers();
        }

        /**
         * @throws InvalidOperationException if timestamp is null
         */

        public long timestamp()
        {
            if (recordContext == null)
            {
                throw new InvalidOperationException("This should not happen as timestamp() should only be called while a record is processed");
            }
            return recordContext.timestamp();
        }


        public Dictionary<string, object> appConfigs()
        {
            Dictionary<string, object> combined = new Dictionary<>();
            combined.putAll(config.originals());
            combined.putAll(config.Values);
            return combined;
        }


        public Dictionary<string, object> appConfigsWithPrefix(string prefix)
        {
            return config.originalsWithPrefix(prefix);
        }


        public void setRecordContext(ProcessorRecordContext recordContext)
        {
            this.recordContext = recordContext;
        }


        public ProcessorRecordContext recordContext()
        {
            return recordContext;
        }


        public void setCurrentNode(ProcessorNode currentNode)
        {
            this.currentNode = currentNode;
        }


        public ProcessorNode currentNode()
        {
            return currentNode;
        }


        public ThreadCache getCache()
        {
            return cache;
        }


        public void initialize()
        {
            initialized = true;
        }


        public void uninitialize()
        {
            initialized = false;
        }
    }
}