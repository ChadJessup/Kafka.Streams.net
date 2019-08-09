using Kafka.Common.Utils;
using Kafka.Streams.Processor.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    /**
     * An in-memory LRU cache store based on HashSet and Dictionary.
     */
    public class MemoryLRUCache : IKeyValueStore<Bytes, byte[]>
    {
        private string name;
        protected Dictionary<Bytes, byte[]> map;

        private bool restoring = false; // TODO: this is a sub-optimal solution to avoid logging during restoration.
                                        // in the future we should augment the StateRestoreCallback with onComplete etc to better resolve this.
        private volatile bool open = true;

        private IEldestEntryRemovalListener listener;

        public MemoryLRUCache(string name, int maxCacheSize)
        {
            //        this.name = name;

            //            // leave room for one extra entry to handle.Adding an entry before the oldest can be removed
            //            this.map = new Dictionary<Bytes, byte[]>(maxCacheSize + 1, 1.01f, true);
            //{
            //            private static long serialVersionUID = 1L;


            //            protected bool removeEldestEntry(KeyValuePair<Bytes, byte[]> eldest)
            //{
            //                bool evict = base.size() > maxCacheSize;
            //                if (evict && !restoring && listener != null)
            //{
            //                    listener.apply(eldest.Key, eldest.Value);
            //                }
            //                return evict;
            //            }
            //        };
            //    }

            //    void setWhenEldestRemoved(EldestEntryRemovalListener listener)
            //{
            //        this.listener = listener;
            //    }

            //    public override string name()
            //{
            //        return this.name;
            //    }

            //public override void init(
            //    IProcessorContext<K, V> context,
            //    IStateStore root)
            //{

            //    // register the store
            //    context.register(root, (key, value) =>
            //{
            //restoring = true;
            //put(Bytes.wrap(key), value);
            //restoring = false;
            //});
            //}

            //    public override bool persistent()
            //{
            //        return false;
            //    }

            //    public override bool isOpen()
            //{
            //        return open;
            //    }

            //[MethodImpl(MethodImplOptions.Synchronized)]
            //public override byte[] get(Bytes key)
            //{
            //        Objects.requireNonNull(key);

            //        return this.map[key];
            //    }

            //    public override synchronized void put(Bytes key, byte[] value)
            //{
            //        Objects.requireNonNull(key);
            //        if (value == null)
            //{
            //            delete(key);
            //        } else
            //{
            //            this.map.Add(key, value);
            //        }
            //    }

            //    public override synchronized byte[] putIfAbsent(Bytes key, byte[] value)
            //{
            //        Objects.requireNonNull(key);
            //        byte[] originalValue = get(key);
            //        if (originalValue == null)
            //{
            //            put(key, value);
            //        }
            //        return originalValue;
            //    }

            //    public override void putAll(List<KeyValue<Bytes, byte[]>> entries)
            //{
            //        foreach (KeyValue<Bytes, byte[]> entry in entries)
            //{
            //            put(entry.key, entry.value);
            //        }
            //    }

            //    public override synchronized byte[] delete(Bytes key)
            //{
            //        Objects.requireNonNull(key);
            //        return this.map.Remove(key);
            //    }

            ///**
            // * @throws InvalidOperationException at every invocation
            // */
            //public override KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to)
            //{
            //    throw new InvalidOperationException("MemoryLRUCache does not support range() function.");
            //}

            //    /**
            //     * @throws InvalidOperationException at every invocation
            //     */
            //    public override KeyValueIterator<Bytes, byte[]> all()
            //{
            //        throw new InvalidOperationException("MemoryLRUCache does not support all() function.");
            //    }

            //    public override long approximateNumEntries()
            //{
            //        return this.map.size();
            //    }

            //public override void flush()
            //{
            //    // do-nothing since it is in-memory
            //}

            //    public override void close()
            //{
            //        open = false;
            //    }

            //public int size()
            //{
            //    return this.map.size();
            //}
        }

        public void put(Bytes key, byte[] value)
        {
            throw new System.NotImplementedException();
        }

        public byte[] putIfAbsent(Bytes key, byte[] value)
        {
            throw new System.NotImplementedException();
        }

        public void putAll(List<KeyValue<Bytes, byte[]>> entries)
        {
            throw new System.NotImplementedException();
        }

        public byte[] delete(Bytes key)
        {
            throw new System.NotImplementedException();
        }

        string IStateStore.name()
        {
            throw new System.NotImplementedException();
        }

        public void init(IProcessorContext<K, V> context, IStateStore root)
        {
            throw new System.NotImplementedException();
        }

        public void flush()
        {
            throw new System.NotImplementedException();
        }

        public void close()
        {
            throw new System.NotImplementedException();
        }

        public bool persistent()
        {
            throw new System.NotImplementedException();
        }

        public bool isOpen()
        {
            throw new System.NotImplementedException();
        }
    }
}