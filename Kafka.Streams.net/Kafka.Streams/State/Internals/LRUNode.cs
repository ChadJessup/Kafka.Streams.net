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
using Kafka.Streams.Processor.Internals.Metrics;

namespace Kafka.Streams.State.Internals
{
    /**
     * A simple wrapper to implement a doubly-linked list around MemoryLRUCacheBytesEntry
     */
    public class LRUNode
    {
        private Bytes key;
        private LRUCacheEntry entry;
        private LRUNode previous;
        private LRUNode next;
        private StreamsMetricsImpl metrics;

        LRUNode(Bytes key, LRUCacheEntry entry)
        {
            this.key = key;
            this.entry = entry;
        }

        long size()
        {
            return key[].Length +
                8 + // entry
                8 + // previous
                8 + // next
                entry.size();
        }

        private void update(LRUCacheEntry entry)
        {
            this.entry = entry;
        }
    }
}
