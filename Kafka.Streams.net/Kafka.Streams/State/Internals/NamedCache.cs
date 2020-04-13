using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.State.Internals
{
    public class NamedCache
    {
        private static readonly ILogger log = new LoggerFactory().CreateLogger<NamedCache>();
        private readonly string Name;
        private readonly ConcurrentDictionary<Bytes, LRUNode> cache = new ConcurrentDictionary<Bytes, LRUNode>();
        private readonly HashSet<Bytes> dirtyKeys = new HashSet<Bytes>();
        private Action<IEnumerable<DirtyEntry>> listener;
        private LRUNode _tail;
        private LRUNode _head;
        private long currentSizeBytes;
        //private NamedCacheMetrics namedCacheMetrics;

        // internal stats
        private long numReadHits = 0;
        private long numReadMisses = 0;
        private long numOverwrites = 0;
        private long numFlushes = 0;

        public NamedCache(string Name)//, StreamsMetricsImpl metrics)
        {
            this.Name = Name;
           // this.metrics = metrics;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public LRUCacheEntry Get(Bytes key)
        {
            if (key == null)
            {
                return null;
            }

            LRUNode node = this.GetInternal(key);
            if (node == null)
            {
                return null;
            }
            this.UpdateLRU(node);
            return node.entry;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void SetListener(Action<IEnumerable<DirtyEntry>> listener)
        {
            this.listener = listener;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Flush()
        {
            this.Flush(null);
        }

        private void Flush(LRUNode evicted)
        {
            this.numFlushes++;

            //if (log.isTraceEnabled())
            //{
            //    log.LogTrace("Named cache {} stats on Flush: #hits={}, #misses={}, #overwrites={}, #flushes={}",
            //        Name, hits(), misses(), overwrites(), flushes());
            //}

            //if (listener == null)
            //{
            //    throw new System.ArgumentException("No listener for namespace " + Name + " registered with cache")
            //    {

            //    }

            //if (dirtyKeys.IsEmpty())
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
        public void Put(Bytes key, LRUCacheEntry value)
        {
            if (!value.isDirty && this.dirtyKeys.Contains(key))
            {
                throw new InvalidOperationException(
                    string.Format(
                        "Attempting to Put a clean entry for key [%s] into NamedCache [%s] when it already contains a dirty entry for the same key",
                        key, this.Name
                    )
                );
            }
            LRUNode node = this.cache[key];
            if (node != null)
            {
                this.numOverwrites++;

                this.currentSizeBytes -= node.Size();
                node.Update(value);
                this.UpdateLRU(node);
            }
            else
            {
                node = new LRUNode(key, value);
                // Put element
                this.PutHead(node);
                this.cache.TryAdd(key, node);
            }
            if (value.isDirty)
            {
                // first Remove and then.Add so we can maintain ordering as the arrival order of the records.
                this.dirtyKeys.Remove(key);
                this.dirtyKeys.Add(key);
            }
            this.currentSizeBytes += node.Size();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public long SizeInBytes()
        {
            return this.currentSizeBytes;
        }

        private LRUNode GetInternal(Bytes key)
        {
            LRUNode node = this.cache[key];
            if (node == null)
            {
                this.numReadMisses++;

                return null;
            }
            else
            {
                this.numReadHits++;
                //namedCacheMetrics.hitRatioSensor.record((double)numReadHits / (double)(numReadHits + numReadMisses));
            }
            return node;
        }

        private void UpdateLRU(LRUNode node)
        {
            this.Remove(node);

            this.PutHead(node);
        }

        private void Remove(LRUNode node)
        {
            if (node.previous != null)
            {
                node.previous.next = node.next;
            }
            else
            {
                this._head = node.next;
            }
            if (node.next != null)
            {
                node.next.previous = node.previous;
            }
            else
            {
                this._tail = node.previous;
            }
        }

        private void PutHead(LRUNode node)
        {
            node.next = this._head;
            node.previous = null;
            if (this._head != null)
            {
                this._head.previous = node;
            }
            this._head = node;
            if (this._tail == null)
            {
                this._tail = this._head;
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Evict()
        {
            if (this._tail == null)
            {
                return;
            }
            LRUNode eldest = this._tail;
            this.currentSizeBytes -= eldest.Size();
            this.Remove(eldest);
            this.cache.TryRemove(eldest.key, out var _);

            if (eldest.entry.isDirty)
            {
                this.Flush(eldest);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public LRUCacheEntry PutIfAbsent(Bytes key, LRUCacheEntry value)
        {
            LRUCacheEntry originalValue = this.Get(key);
            if (originalValue == null)
            {
                this.Put(key, value);
            }
            return originalValue;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public LRUCacheEntry Delete(Bytes key)
        {
            if (!this.cache.TryRemove(key, out var node) || node == null)
            {
                return null;
            }

            this.Remove(node);
            this.dirtyKeys.Remove(key);
            this.currentSizeBytes -= node.Size();
            return node.entry;
        }

        public long Size()
        {
            return this.cache.Count;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IEnumerator<KeyValuePair<Bytes, LRUNode>> SubMapIterator(Bytes from, Bytes to)
        {
            return null;// cache.subMap(from, true, to, true).iterator();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public IEnumerator<KeyValuePair<Bytes, LRUNode>> AllIterator()
        {
            return this.cache.GetEnumerator();
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Close()
        {
            this._head = this._tail = null;
            this.listener = null;
            this.currentSizeBytes = 0;
            this.dirtyKeys.Clear();
            this.cache.Clear();
            //namedCacheMetrics.removeAllSensors();
        }
    }
}
