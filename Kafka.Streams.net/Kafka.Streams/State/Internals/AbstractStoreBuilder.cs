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
using Kafka.Streams.Processor.Interfaces;

namespace Kafka.Streams.State.Internals
{
    public abstract class AbstractStoreBuilder<K, V, T> : IStoreBuilder<T>
        where T : IStateStore
    {
        private Dictionary<string, string> logConfig = new Dictionary<>();
        protected string name;
        ISerde<K> keySerde;
        ISerde<V> valueSerde;
        ITime time;
        bool enableCaching;
        bool enableLogging = true;

        public AbstractStoreBuilder(string name,
                                    ISerde<K> keySerde,
                                    ISerde<V> valueSerde,
                                    ITime time)
        {
            name = name ?? throw new System.ArgumentNullException("name cannot be null", nameof(name));
            time = time ?? throw new System.ArgumentNullException("time cannot be null", nameof(time));
            this.name = name;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.time = time;
        }

        public override IStoreBuilder<T> withCachingEnabled()
        {
            enableCaching = true;
            return this;
        }

        public override IStoreBuilder<T> withCachingDisabled()
        {
            enableCaching = false;
            return this;
        }

        public override IStoreBuilder<T> withLoggingEnabled(Dictionary<string, string> config)
        {
            config = config ?? throw new System.ArgumentNullException("config can't be null", nameof(config));
            enableLogging = true;
            logConfig = config;
            return this;
        }

        public override IStoreBuilder<T> withLoggingDisabled()
        {
            enableLogging = false;
            logConfig.clear();
            return this;
        }

        public override Dictionary<string, string> logConfig()
        {
            return logConfig;
        }

        public override bool loggingEnabled()
        {
            return enableLogging;
        }

        public override string name()
        {
            return name;
        }
    }
}