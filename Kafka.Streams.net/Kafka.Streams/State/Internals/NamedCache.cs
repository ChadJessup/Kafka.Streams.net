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
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Kafka.Common.Utils;
using Kafka.Streams.Processor.Internals.Metrics;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.State.Internals
{
    public class NamedCache
    {
        private static readonly ILogger log = new LoggerFactory().CreateLogger<NamedCache>();
        private readonly string name;
        private readonly ConcurrentDictionary<Bytes, LRUNode> cache = new ConcurrentDictionary<Bytes, LRUNode>();
        private readonly HashSet<Bytes> dirtyKeys = new HashSet<Bytes>();
        private IDirtyEntryFlushListener listener;
        private LRUNode _tail;
        private LRUNode _head;
        private long currentSizeBytes;
        //private NamedCacheMetrics namedCacheMetrics;

        // internal stats
        private long numReadHits = 0;
        private long numReadMisses = 0;
        private long numOverwrites = 0;
        private long numFlushes = 0;

        public NamedCache(string name, StreamsMetricsImpl metrics)
        {
            this.name = name;
           // this.metrics = metrics;
        }

        //public NamedCache(string name, StreamsMetricsImpl metrics)
        //{
        //    this.name = name;
        //    //this.namedCacheMetrics = new NamedCacheMetrics(metrics, name);
        //}

        [MethodImpl(MethodImplOptions.Synchronized)]
        long hits()
        {
            return numReadHits;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        long misses()
        {
            return numReadMisses;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        long overwrites()
        {
            return numOverwrites;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        long flushes()
        {
            return numFlushes;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public LRUCacheEntry get(Bytes key)
        {
            if (key == null)
            {
                return null;
            }

            LRUNode node = getInternal(key);
            if (node == null)
            {
                return null;
            }
            updateLRU(node);
            return node.entry;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void setListener(IDirtyEntryFlushListener listener)
        {
            this.listener = listener;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void flush()
        {
            flush(null);
        }

        private void flush(LRUNode evicted)
        {
            numFlushes++;

            //if (log.isTraceEnabled())
            //{
            //    log.LogTrace("Named cache {} stats on flush: #hits={}, #misses={}, #overwrites={}, #flushes={}",
            //        name, hits(), misses(), overwrites(), flushes());
            //}

            //if (listener == null)
            //{
            //    throw new System.ArgumentException("No listener for namespace " + name + " registered with cache")
            //    {

            //    }

            //if (dirtyKeys.isEmpty())
            //    {
            //        return;
            //    }

            //    List<ThreadCache.DirtyEntry> entries = new List<>();
            //    List<Bytes> deleted = new List<>();

            //    // evicted already been removed from the cache so.Add it to the list of
            //    // flushed entries and Remove from dirtyKeys.
            //    if (evicted != null)
            //    {
            //        entries.Add(new DirtyEntry(evicted.key, evicted.entry.value(), evicted.entry));
            //        dirtyKeys.Remove(evicted.key);
            //    }

            //    foreach (Bytes key in dirtyKeys)
            //    {
            //        LRUNode node = getInternal(key);
            //        if (node == null)
            //        {
            //            throw new InvalidOperationException("Key = " + key + " found in dirty key set, but entry is null");
            //        }
            //        entries.Add(new DirtyEntry(key, node.entry.value(), node.entry));
            //        node.entry.markClean();
            //        if (node.entry.value() == null)
            //        {
            //            deleted.Add(node.key);
            //        }
            //    }
            //    // clear dirtyKeys before the listener is applied as it may be re-entrant.
            //    dirtyKeys.clear();
            //    listener.apply(entries);
            //    foreach (Bytes key in deleted)
            //    {
            //        delete(key);
            //    }
        }


        [MethodImpl(MethodImplOptions.Synchronized)]
        public void put(Bytes key, LRUCacheEntry value)
        {
            if (!value.isDirty && dirtyKeys.Contains(key))
            {
                throw new InvalidOperationException(
                    string.Format(
                        "Attempting to put a clean entry for key [%s] into NamedCache [%s] when it already contains a dirty entry for the same key",
                        key, name
                    )
                );
            }
            LRUNode node = cache[key];
            if (node != null)
            {
                numOverwrites++;

                currentSizeBytes -= node.size();
                node.update(value);
                updateLRU(node);
            }
            else
            {
                node = new LRUNode(key, value);
                // put element
                putHead(node);
                cache.TryAdd(key, node);
            }
            if (value.isDirty)
            {
                // first Remove and then.Add so we can maintain ordering as the arrival order of the records.
                dirtyKeys.Remove(key);
                dirtyKeys.Add(key);
            }
            currentSizeBytes += node.size();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public long sizeInBytes()
        {
            return currentSizeBytes;
        }

        private LRUNode getInternal(Bytes key)
        {
            LRUNode node = cache[key];
            if (node == null)
            {
                numReadMisses++;

                return null;
            }
            else
            {
                numReadHits++;
                //namedCacheMetrics.hitRatioSensor.record((double)numReadHits / (double)(numReadHits + numReadMisses));
            }
            return node;
        }

        private void updateLRU(LRUNode node)
        {
            Remove(node);

            putHead(node);
        }

        private void Remove(LRUNode node)
        {
            if (node.previous != null)
            {
                node.previous.next = node.next;
            }
            else
            {
                _head = node.next;
            }
            if (node.next != null)
            {
                node.next.previous = node.previous;
            }
            else
            {
                _tail = node.previous;
            }
        }

        private void putHead(LRUNode node)
        {
            node.next = _head;
            node.previous = null;
            if (_head != null)
            {
                _head.previous = node;
            }
            _head = node;
            if (_tail == null)
            {
                _tail = _head;
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void evict()
        {
            if (_tail == null)
            {
                return;
            }
            LRUNode eldest = _tail;
            currentSizeBytes -= eldest.size();
            Remove(eldest);
            cache.TryRemove(eldest.key, out var _);

            if (eldest.entry.isDirty)
            {
                flush(eldest);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public LRUCacheEntry putIfAbsent(Bytes key, LRUCacheEntry value)
        {
            LRUCacheEntry originalValue = get(key);
            if (originalValue == null)
            {
                put(key, value);
            }
            return originalValue;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        void putAll(List<KeyValue<byte[], LRUCacheEntry>> entries)
        {
            foreach (KeyValue<byte[], LRUCacheEntry> entry in entries)
            {
                put(Bytes.wrap(entry.Key), entry.Value);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public LRUCacheEntry delete(Bytes key)
        {
            if (!cache.TryRemove(key, out var node) || node == null)
            {
                return null;
            }

            Remove(node);
            dirtyKeys.Remove(key);
            currentSizeBytes -= node.size();
            return node.entry;
        }

        public long size()
        {
            return cache.Count;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IEnumerator<KeyValuePair<Bytes, LRUNode>> subMapIterator(Bytes from, Bytes to)
        {
            return null;// cache.subMap(from, true, to, true).iterator();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IEnumerator<KeyValuePair<Bytes, LRUNode>> allIterator()
        {
            return cache.GetEnumerator();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        LRUCacheEntry first()
        {
            if (_head == null)
            {
                return null;
            }

            return _head.entry;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        LRUCacheEntry last()
        {
            if (_tail == null)
            {
                return null;
            }

            return _tail.entry;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        LRUNode head()
        {
            return _head;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        LRUNode tail()
        {
            return _tail;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void close()
        {
            _head = _tail = null;
            listener = null;
            currentSizeBytes = 0;
            dirtyKeys.Clear();
            cache.Clear();
            //namedCacheMetrics.removeAllSensors();
        }
    }
}
