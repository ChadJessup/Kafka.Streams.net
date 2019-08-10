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
using Kafka.Common.Utils;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.State.Internals
{

    public class MemoryNavigableLRUCache : MemoryLRUCache
    {

        private static ILogger LOG = new LoggerFactory().CreateLogger<MemoryNavigableLRUCache>();

        public MemoryNavigableLRUCache(string name, int maxCacheSize)
            : base(name, maxCacheSize)
        {
        }

        public override IKeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to)
        {

            if (from.CompareTo(to) > 0)
            {
                LOG.LogWarning("Returning empty iterator for fetch with invalid key range: from > to. "
                    + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
                return KeyValueIterators.emptyIterator();
            }

            TreeMap<Bytes, byte[]> treeMap = toTreeMap();
            return new DelegatingPeekingKeyValueIterator<>(name,
                new MemoryNavigableLRUCache.CacheIterator(treeMap.navigableKeySet()
                    .subSet(from, true, to, true).iterator(), treeMap));
        }

        public override IKeyValueIterator<Bytes, byte[]> all()
        {
            TreeMap<Bytes, byte[]> treeMap = toTreeMap();
            return new MemoryNavigableLRUCache.CacheIterator(treeMap.navigableKeySet().iterator(), treeMap);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private TreeMap<Bytes, byte[]> toTreeMap()
        {
            return new TreeMap<>(this.map);
        }
    }
}
