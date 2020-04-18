//using Kafka.Common.Utils;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.Interfaces;
//using System.Collections.Generic;

//namespace Kafka.Streams.State.Internals
//{
//    /**
//     * An in-memory LRU cache store based on HashSet and Dictionary.
//     */
//    public class MemoryLRUCache : IKeyValueStore<Bytes, byte[]>
//    {
//        private string Name;
//        protected Dictionary<Bytes, byte[]> map;

//        private bool restoring = false; // TODO: this is a sub-optimal solution to avoid logging during restoration.
//                                        // in the future we should augment the StateRestoreCallback with onComplete etc to better resolve this.
//        private volatile bool open = true;

//        private IEldestEntryRemovalListener listener;

//        public MemoryLRUCache(string Name, int maxCacheSize)
//        {
//            //        this.Name = Name;

//            //            // leave room for one extra entry to handle.Adding an entry before the oldest can be removed
//            //            this.Map = new Dictionary<Bytes, byte[]>(maxCacheSize + 1, 1.01f, true);
//            //{
//            //            private static long serialVersionUID = 1L;


//            //            protected bool removeEldestEntry(KeyValuePair<Bytes, byte[]> eldest)
//            //{
//            //                bool evict = base.size() > maxCacheSize;
//            //                if (evict && !restoring && listener != null)
//            //{
//            //                    listener.apply(eldest.Key, eldest.Value);
//            //                }
//            //                return evict;
//            //            }
//            //        };
//            //    }

//            //    void setWhenEldestRemoved(EldestEntryRemovalListener listener)
//            //{
//            //        this.listener = listener;
//            //    }

//            //    public override string Name
//            //{
//            //        return this.Name;
//            //    }

//            //public override void Init(
//            //    IProcessorContext context,
//            //    IStateStore root)
//            //{

//            //    // register the store
//            //    context.register(root, (key, value) =>
//            //{
//            //restoring = true;
//            //Put(Bytes.Wrap(key), value);
//            //restoring = false;
//            //});
//            //}

//            //    public override bool Persistent()
//            //{
//            //        return false;
//            //    }

//            //    public override bool IsOpen()
//            //{
//            //        return open;
//            //    }

//            //[MethodImpl(MethodImplOptions.Synchronized)]
//            //public override byte[] get(Bytes key)
//            //{
//            //        Objects.requireNonNull(key);

//            //        return this.Map[key];
//            //    }
//            //[MethodImpl(MethodImplOptions.Synchronized)]
//            //    public override void Put(Bytes key, byte[] value)
//            //{
//            //        Objects.requireNonNull(key);
//            //        if (value == null)
//            //{
//            //            delete(key);
//            //        } else
//            //{
//            //            this.Map.Add(key, value);
//            //        }
//            //    }
//            //  [MethodImpl(MethodImplOptions.Synchronized)]
//            //    public override byte[] putIfAbsent(Bytes key, byte[] value)
//            //{
//            //        Objects.requireNonNull(key);
//            //        byte[] originalValue = get(key);
//            //        if (originalValue == null)
//            //{
//            //            Put(key, value);
//            //        }
//            //        return originalValue;
//            //    }

//            //    public override void putAll(List<KeyValuePair<Bytes, byte[]>> entries)
//            //{
//            //        foreach (KeyValuePair<Bytes, byte[]> entry in entries)
//            //{
//            //            Put(entry.key, entry.value);
//            //        }
//            //    }
//            //   [MethodImpl(MethodImplOptions.Synchronized)]
//            //    public override byte[] delete(Bytes key)
//            //{
//            //        Objects.requireNonNull(key);
//            //        return this.Map.Remove(key);
//            //    }

//            ///**
//            // * @throws InvalidOperationException at every invocation
//            // */
//            //public override KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to)
//            //{
//            //    throw new InvalidOperationException("MemoryLRUCache does not support range() function.");
//            //}

//            //    /**
//            //     * @throws InvalidOperationException at every invocation
//            //     */
//            //    public override KeyValueIterator<Bytes, byte[]> All()
//            //{
//            //        throw new InvalidOperationException("MemoryLRUCache does not support All() function.");
//            //    }

//            //    public override long approximateNumEntries
//            //{
//            //        return this.Map.size();
//            //    }

//            //public override void Flush()
//            //{
//            //    // do-nothing since it is in-memory
//            //}

//            //    public override void Close()
//            //{
//            //        open = false;
//            //    }

//            //public int size()
//            //{
//            //    return this.Map.size();
//            //}
//        }

//        public void Put(Bytes key, byte[] value)
//        {
//            throw new System.NotImplementedException();
//        }

//        public byte[] putIfAbsent(Bytes key, byte[] value)
//        {
//            throw new System.NotImplementedException();
//        }

//        public void putAll(List<KeyValuePair<Bytes, byte[]>> entries)
//        {
//            throw new System.NotImplementedException();
//        }

//        public byte[] delete(Bytes key)
//        {
//            throw new System.NotImplementedException();
//        }

//        string IStateStore.Name => throw new System.NotImplementedException();

//        public void Init(IProcessorContext<Bytes, byte[]> context, IStateStore root)
//        {
//            throw new System.NotImplementedException();
//        }

//        public void Flush()
//        {
//            throw new System.NotImplementedException();
//        }

//        public void Close()
//        {
//            throw new System.NotImplementedException();
//        }

//        public bool Persistent()
//        {
//            throw new System.NotImplementedException();
//        }

//        public bool IsOpen()
//        {
//            throw new System.NotImplementedException();
//        }

//        public void Init<K, V>(IProcessorContext context, IStateStore root)
//        {
//            throw new System.NotImplementedException();
//        }

//        public byte[] get(Bytes key)
//        {
//            throw new System.NotImplementedException();
//        }

//        public IKeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to)
//        {
//            throw new System.NotImplementedException();
//        }

//        public IKeyValueIterator<Bytes, byte[]> All()
//        {
//            throw new System.NotImplementedException();
//        }

//        public long approximateNumEntries
//        {
//            throw new System.NotImplementedException();
//        }
//    }
//}