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
using Kafka.Streams.Processors.Interfaces;
using NodaTime;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public abstract class AbstractStoreBuilder<K, V, T> : IStoreBuilder<T>
        where T : IStateStore
    {
        public Dictionary<string, string> logConfig { get; private set; } = new Dictionary<string, string>();
        public string name { get; }
        public bool loggingEnabled { get; }

        public ISerde<K> keySerde { get; }
        public ISerde<V> valueSerde { get; }
        public IClock clock { get; }
        public bool enableCaching { get; private set; }
        public bool enableLogging { get; private set; } = true;

        public AbstractStoreBuilder(
            string name,
            ISerde<K> keySerde,
            ISerde<V> valueSerde,
            IClock clock)
        {
            name = name ?? throw new ArgumentNullException(nameof(name));
            clock = clock ?? throw new ArgumentNullException(nameof(clock));

            this.name = name;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.clock = clock;
        }

        public IStoreBuilder<T> WithCachingEnabled()
        {
            enableCaching = true;

            return this;
        }

        public IStoreBuilder<T> WithCachingDisabled()
        {
            enableCaching = false;

            return this;
        }

        public IStoreBuilder<T> WithLoggingEnabled(Dictionary<string, string> config)
        {
            config = config ?? throw new ArgumentNullException(nameof(config));

            enableLogging = true;
            logConfig = config;


            return this;
        }

        public IStoreBuilder<T> WithLoggingDisabled()
        {
            enableLogging = false;
            logConfig.Clear();

            return this;
        }

        public abstract T Build();
    }
}