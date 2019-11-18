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
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class MaterializedInternal<K, V, S> : Materialized<K, V, S>
        where S : IStateStore
    {
        private readonly bool queriable;

        public MaterializedInternal(Materialized<K, V, S> materialized)
            : this(materialized, null, null)
        {
        }

        public MaterializedInternal(
            Materialized<K, V, S> materialized,
            IInternalNameProvider nameProvider,
            string generatedStorePrefix)
            : base(materialized)
        {
            // if storeName is not provided, the corresponding KTable would never be queryable;
            // but we still need to provide an internal name for it in case we materialize.
            queriable = this.StoreName != null;

            if (!queriable && nameProvider != null)
            {
                this.StoreName = nameProvider.NewStoreName(generatedStorePrefix);
            }
        }

        public string queryableStoreName()
        {
            return queriable
                ? this.StoreName
                : null;
        }

        private string _storeName;
        public override string StoreName
        {
            get => StoreSupplier?.name ?? _storeName;
            protected set => _storeName = value;
        }

        public Dictionary<string, string> logConfig()
        {
            return TopicConfig;
        }

        public static explicit operator MaterializedInternal<K, V, S>(MaterializedInternal<K, V, IStateStore> v)
        {
            throw new NotImplementedException();
        }
    }
}