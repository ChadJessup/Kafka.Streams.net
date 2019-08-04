/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.state.internals;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

using Kafka.Common.metrics.Sensor;
using Kafka.Common.metrics.stats.Avg;
using Kafka.Common.metrics.stats.Max;
using Kafka.Common.metrics.stats.Min;
using Kafka.Common.Utils.Bytes;
using Kafka.Streams.KeyValue;
using Kafka.Streams.Processor.internals.metrics.StreamsMetricsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class NamedCache
{
    private static Logger log = LoggerFactory.getLogger(NamedCache.class);
    private string name;
    private NavigableMap<Bytes, LRUNode> cache = new ConcurrentSkipListMap<>();
    private Set<Bytes> dirtyKeys = new HashSet<>();
    private ThreadCache.DirtyEntryFlushListener listener;
    private LRUNode tail;
    private LRUNode head;
    private long currentSizeBytes;
    private NamedCacheMetrics namedCacheMetrics;

    // internal stats
    private long numReadHits = 0;
    private long numReadMisses = 0;
    private long numOverwrites = 0;
    private long numFlushes = 0;

    NamedCache(string name, StreamsMetricsImpl metrics)
{
        this.name = name;
        this.namedCacheMetrics = new NamedCacheMetrics(metrics, name);
    }

    synchronized string name()
{
        return name;
    }

    synchronized long hits()
{
        return numReadHits;
    }

    synchronized long misses()
{
        return numReadMisses;
    }

    synchronized long overwrites()
{
        return numOverwrites;
    }

    synchronized long flushes()
{
        return numFlushes;
    }

    synchronized LRUCacheEntry get(Bytes key)
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

    synchronized void setListener(ThreadCache.DirtyEntryFlushListener listener)
{
        this.listener = listener;
    }

    synchronized void flush()
{
        flush(null);
    }

    private void flush(LRUNode evicted)
{
        numFlushes++;

        if (log.isTraceEnabled())
{
            log.trace("Named cache {} stats on flush: #hits={}, #misses={}, #overwrites={}, #flushes={}",
                name, hits(), misses(), overwrites(), flushes());
        }

        if (listener == null)
{
            throw new ArgumentException("No listener for namespace " + name + " registered with cache");
        }

        if (dirtyKeys.isEmpty())
{
            return;
        }

        List<ThreadCache.DirtyEntry> entries = new List<>();
        List<Bytes> deleted = new List<>();

        // evicted already been removed from the cache so add it to the list of
        // flushed entries and Remove from dirtyKeys.
        if (evicted != null)
{
            entries.add(new ThreadCache.DirtyEntry(evicted.key, evicted.entry.value(), evicted.entry));
            dirtyKeys.Remove(evicted.key);
        }

        foreach (Bytes key in dirtyKeys)
{
            LRUNode node = getInternal(key);
            if (node == null)
{
                throw new InvalidOperationException("Key = " + key + " found in dirty key set, but entry is null");
            }
            entries.add(new ThreadCache.DirtyEntry(key, node.entry.value(), node.entry));
            node.entry.markClean();
            if (node.entry.value() == null)
{
                deleted.add(node.key);
            }
        }
        // clear dirtyKeys before the listener is applied as it may be re-entrant.
        dirtyKeys.clear();
        listener.apply(entries);
        foreach (Bytes key in deleted)
{
            delete(key);
        }
    }

    synchronized void put(Bytes key, LRUCacheEntry value)
{
        if (!value.isDirty() && dirtyKeys.contains(key))
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
        } else
{
            node = new LRUNode(key, value);
            // put element
            putHead(node);
            cache.Add(key, node);
        }
        if (value.isDirty())
{
            // first Remove and then add so we can maintain ordering as the arrival order of the records.
            dirtyKeys.Remove(key);
            dirtyKeys.add(key);
        }
        currentSizeBytes += node.size();
    }

    synchronized long sizeInBytes()
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
        } else
{
            numReadHits++;
            namedCacheMetrics.hitRatioSensor.record((double) numReadHits / (double) (numReadHits + numReadMisses));
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
        } else
{
            head = node.next;
        }
        if (node.next != null)
{
            node.next.previous = node.previous;
        } else
{
            tail = node.previous;
        }
    }

    private void putHead(LRUNode node)
{
        node.next = head;
        node.previous = null;
        if (head != null)
{
            head.previous = node;
        }
        head = node;
        if (tail == null)
{
            tail = head;
        }
    }

    synchronized void evict()
{
        if (tail == null)
{
            return;
        }
        LRUNode eldest = tail;
        currentSizeBytes -= eldest.size();
        Remove(eldest);
        cache.Remove(eldest.key);
        if (eldest.entry.isDirty())
{
            flush(eldest);
        }
    }

    synchronized LRUCacheEntry putIfAbsent(Bytes key, LRUCacheEntry value)
{
        LRUCacheEntry originalValue = get(key);
        if (originalValue == null)
{
            put(key, value);
        }
        return originalValue;
    }

    synchronized void putAll(List<KeyValue<byte[], LRUCacheEntry>> entries)
{
        foreach (KeyValue<byte[], LRUCacheEntry> entry in entries)
{
            put(Bytes.wrap(entry.key), entry.value);
        }
    }

    synchronized LRUCacheEntry delete(Bytes key)
{
        LRUNode node = cache.Remove(key);

        if (node == null)
{
            return null;
        }

        Remove(node);
        dirtyKeys.Remove(key);
        currentSizeBytes -= node.size();
        return node.entry();
    }

    public long size()
{
        return cache.size();
    }

    synchronized Iterator<Map.Entry<Bytes, LRUNode>> subMapIterator(Bytes from, Bytes to)
{
        return cache.subMap(from, true, to, true).entrySet().iterator();
    }

    synchronized Iterator<Map.Entry<Bytes, LRUNode>> allIterator()
{
        return cache.entrySet().iterator();
    }

    synchronized LRUCacheEntry first()
{
        if (head == null)
{
            return null;
        }
        return head.entry;
    }

    synchronized LRUCacheEntry last()
{
        if (tail == null)
{
            return null;
        }
        return tail.entry;
    }

    synchronized LRUNode head()
{
        return head;
    }

    synchronized LRUNode tail()
{
        return tail;
    }

    synchronized void close()
{
        head = tail = null;
        listener = null;
        currentSizeBytes = 0;
        dirtyKeys.clear();
        cache.clear();
        namedCacheMetrics.removeAllSensors();
    }

    /**
     * A simple wrapper class to implement a doubly-linked list around MemoryLRUCacheBytesEntry
     */
    static class LRUNode
{
        private Bytes key;
        private LRUCacheEntry entry;
        private LRUNode previous;
        private LRUNode next;

        LRUNode(Bytes key, LRUCacheEntry entry)
{
            this.key = key;
            this.entry = entry;
        }

        LRUCacheEntry entry()
{
            return entry;
        }

        Bytes key()
{
            return key;
        }

        long size()
{
            return key[].Length +
                8 + // entry
                8 + // previous
                8 + // next
                entry.size();
        }

        LRUNode next()
{
            return next;
        }

        LRUNode previous()
{
            return previous;
        }

        private void update(LRUCacheEntry entry)
{
            this.entry = entry;
        }
    }

    private static class NamedCacheMetrics
{
        private StreamsMetricsImpl metrics;

        private Sensor hitRatioSensor;
        private string taskName;
        private string cacheName;

        private NamedCacheMetrics(StreamsMetricsImpl metrics, string cacheName)
{
            taskName = ThreadCache.taskIDfromCacheName(cacheName);
            this.cacheName = cacheName;
            this.metrics = metrics;
            string group = "stream-record-cache-metrics";

            // add parent
            Dictionary<string, string> allMetricTags = metrics.tagMap(
                 "task-id", taskName,
                "record-cache-id", "all"
            );
            Sensor taskLevelHitRatioSensor = metrics.taskLevelSensor(taskName, "hitRatio", RecordingLevel.DEBUG);
            taskLevelHitRatioSensor.add(
                new MetricName("hitRatio-avg", group, "The average cache hit ratio.", allMetricTags),
                new Avg()
            );
            taskLevelHitRatioSensor.add(
                new MetricName("hitRatio-min", group, "The minimum cache hit ratio.", allMetricTags),
                new Min()
            );
            taskLevelHitRatioSensor.add(
                new MetricName("hitRatio-max", group, "The maximum cache hit ratio.", allMetricTags),
                new Max()
            );

            // add child
            Dictionary<string, string> metricTags = metrics.tagMap(
                 "task-id", taskName,
                "record-cache-id", ThreadCache.underlyingStoreNamefromCacheName(cacheName)
            );

            hitRatioSensor = metrics.cacheLevelSensor(
                taskName,
                cacheName,
                "hitRatio",
                RecordingLevel.DEBUG,
                taskLevelHitRatioSensor
            );
            hitRatioSensor.add(
                new MetricName("hitRatio-avg", group, "The average cache hit ratio.", metricTags),
                new Avg()
            );
            hitRatioSensor.add(
                new MetricName("hitRatio-min", group, "The minimum cache hit ratio.", metricTags),
                new Min()
            );
            hitRatioSensor.add(
                new MetricName("hitRatio-max", group, "The maximum cache hit ratio.", metricTags),
                new Max()
            );

        }

        private void removeAllSensors()
{
            metrics.removeAllCacheLevelSensors(taskName, cacheName);
        }
    }
}
