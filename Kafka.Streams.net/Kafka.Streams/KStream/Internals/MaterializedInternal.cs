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
using Kafka.Streams.Processor.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals
{
    public class MaterializedInternal<K, V, S> : Materialized<K, V, S>
        where S : IStateStore
    {
        private bool queriable;
        private string _storeName;

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
            queriable = storeName() != null;
            if (!queriable && nameProvider != null)
            {
                _storeName = nameProvider.NewStoreName(generatedStorePrefix);
            }
        }

        public string queryableStoreName()
        {
            return queriable
                ? storeName()
                : null;
        }

        public string storeName()
        {
            if (storeSupplier != null)
            {
                return storeSupplier.name;
            }

            return _storeName;
        }

        public Dictionary<string, string> logConfig()
        {
            return topicConfig;
        }

        public static explicit operator MaterializedInternal<K, V, S>(MaterializedInternal<K, V, IStateStore> v)
        {
            throw new NotImplementedException();
        }
    }
}