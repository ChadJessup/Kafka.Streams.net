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
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Topologies
{
    public class StateStoreFactory
    {
        protected HashSet<string> users { get; } = new HashSet<string>();
    }

    public class StateStoreFactory<T> : StateStoreFactory
        where T : IStateStore
    {
        private readonly IStoreBuilder<T> builder;

        public StateStoreFactory(IStoreBuilder<T> builder)
        {
            this.builder = builder;
        }

        public IStateStore build()
        {
            return builder.build();
        }

        long retentionPeriod<K, V>()
        {
            if (builder is WindowStoreBuilder<K, V>)
            {
                return ((WindowStoreBuilder<K, V>)builder).retentionPeriod();
            }
            else if (builder is TimestampedWindowStoreBuilder<K, V>)
            {
                return ((TimestampedWindowStoreBuilder<K, V>)builder).retentionPeriod();
            }
            //else if (builder is SessionStoreBuilder<K, V>)
            //{
            //    return ((SessionStoreBuilder<K, V>)builder).retentionPeriod();
            //}
            else
            {
                throw new InvalidOperationException("retentionPeriod is not supported when not a window store");
            }
        }

        public string name => builder.name;

        private bool isWindowStore<K, V>()
        {
            return false;
            //builder is WindowStoreBuilder<K, V>
            //    || builder is TimestampedWindowStoreBuilder<K, V>
            //    || builder is SessionStoreBuilder<K, V>;
        }
    }
}
