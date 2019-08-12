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
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public abstract class AbstractStoreBuilder<K, V, T> : IStoreBuilder<T>
        where T : IStateStore
    {
        public Dictionary<string, string> logConfig { get; private set; } = new Dictionary<string, string>();
        public string name { get; }
        public bool loggingEnabled { get; }

        ISerde<K> keySerde;
        ISerde<V> valueSerde;
        ITime time;
        bool enableCaching;
        bool enableLogging = true;

        public AbstractStoreBuilder(
            string name,
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

        public IStoreBuilder<T> withCachingEnabled()
        {
            enableCaching = true;
            return this;
        }

        public IStoreBuilder<T> withCachingDisabled()
        {
            enableCaching = false;
            return this;
        }

        public IStoreBuilder<T> withLoggingEnabled(Dictionary<string, string> config)
        {
            config = config ?? throw new System.ArgumentNullException("config can't be null", nameof(config));
            enableLogging = true;
            logConfig = config;
            return this;
        }

        public IStoreBuilder<T> withLoggingDisabled()
        {
            enableLogging = false;
            logConfig.Clear();
            return this;
        }

        public bool loggingEnabled
        {
            return enableLogging;
        }

        public abstract T build();
    }
}