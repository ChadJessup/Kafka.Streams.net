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
using Kafka.Streams.Processors.Internals.Metrics;

namespace Kafka.Streams.State.Internals
{
    /**
     * A simple wrapper to implement a doubly-linked list around MemoryLRUCacheBytesEntry
     */
    public class LRUNode
    {
        public Bytes key { get; }
        public LRUCacheEntry entry { get; private set; }
        public LRUNode previous { get; set; }
        public LRUNode next { get; set; }
        private readonly StreamsMetricsImpl metrics;

        public LRUNode(Bytes key, LRUCacheEntry entry)
        {
            this.key = key;
            this.entry = entry;
        }

        public long size()
        {
            return key.get().Length +
                8 + // entry
                8 + // previous
                8 + // next
                entry.size();
        }

        public void update(LRUCacheEntry entry)
        {
            this.entry = entry;
        }
    }
}
