//using Kafka.Streams;
//using Kafka.Streams.State.Internals;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class ThreadCacheTest
//    {
//        string ns = "0.0-ns";
//        string namespace1 = "0.1-ns";
//        string namespace2 = "0.2-ns";
//        private LogContext logContext = new LogContext("testCache ");

//        [Fact]
//        public void BasicPutGet()
//        { //throws IOException
//            List<KeyValuePair<string, string>> toInsert = Arrays.asList(
//                    new KeyValuePair<string, string>("K1", "V1"),
//                    new KeyValuePair<string, string>("K2", "V2"),
//                    new KeyValuePair<string, string>("K3", "V3"),
//                    new KeyValuePair<string, string>("K4", "V4"),
//                    new KeyValuePair<string, string>("K5", "V5"));
//            KeyValuePair<string, string> kv = toInsert.Get(0);
//            ThreadCache cache = new ThreadCache(logContext,
//                                                      toInsert.Count * memoryCacheEntrySize(kv.Key.getBytes(), kv.Value.getBytes(), ""),
//                                                      new MockStreamsMetrics(new Metrics()));

//            foreach (KeyValuePair<string, string> kvToInsert in toInsert)
//            {
//                Bytes key = Bytes.Wrap(kvToInsert.Key.getBytes());
//                byte[] value = kvToInsert.Value.getBytes();
//                cache.Put(ns, key, new LRUCacheEntry(value, null, true, 1L, 1L, 1, ""));
//            }

//            foreach (KeyValuePair<string, string> kvToInsert in toInsert)
//            {
//                Bytes key = Bytes.Wrap(kvToInsert.Key.getBytes());
//                LRUCacheEntry entry = cache.Get(ns, key);
//                Assert.Equal(entry.isDirty(), true);
//                Assert.Equal(new string(entry.Value), kvToInsert.Value);
//            }

//            Assert.Equal(cache.gets(), 5);
//            Assert.Equal(cache.puts(), 5);
//            Assert.Equal(cache.evicts(), 0);
//            Assert.Equal(cache.flushes(), 0);
//        }

//        private void CheckOverheads(double entryFactor,
//                                    double systemFactor,
//                                    long desiredCacheSize,
//                                    int keySizeBytes,
//                                    int valueSizeBytes)
//        {
//            Runtime runtime = Runtime.getRuntime();
//            long numElements = desiredCacheSize / memoryCacheEntrySize(new byte[keySizeBytes], new byte[valueSizeBytes], "");

//            System.gc();
//            long prevRuntimeMemory = runtime.totalMemory() - runtime.freeMemory();

//            ThreadCache cache = new ThreadCache(logContext, desiredCacheSize, new MockStreamsMetrics(new Metrics()));
//            long size = cache.sizeBytes();
//            Assert.Equal(size, 0);
//            for (int i = 0; i < numElements; i++)
//            {
//                string keyStr = "K" + i;
//                Bytes key = Bytes.Wrap(keyStr.getBytes());
//                byte[] value = new byte[valueSizeBytes];
//                cache.Put(ns, key, new LRUCacheEntry(value, null, true, 1L, 1L, 1, ""));
//            }


//            System.gc();
//            double ceiling = desiredCacheSize + desiredCacheSize * entryFactor;
//            long usedRuntimeMemory = runtime.totalMemory() - runtime.freeMemory() - prevRuntimeMemory;
//            Assert.True((double)cache.sizeBytes() <= ceiling);

//            Assert.True("Used memory size " + usedRuntimeMemory + " greater than expected " + cache.sizeBytes() * systemFactor,
//                cache.sizeBytes() * systemFactor >= usedRuntimeMemory);
//        }

//        [Fact]
//        public void CacheOverheadsSmallValues()
//        {
//            Runtime runtime = Runtime.getRuntime();
//            double factor = 0.05;
//            double systemFactor = 3; // if I ask for a cache size of 10 MB, accept an overhead of 3x, i.e., 30 MBs might be allocated
//            long desiredCacheSize = Math.min(100 * 1024 * 1024L, runtime.maxMemory());
//            int keySizeBytes = 8;
//            int valueSizeBytes = 100;

//            checkOverheads(factor, systemFactor, desiredCacheSize, keySizeBytes, valueSizeBytes);
//        }

//        [Fact]
//        public void CacheOverheadsLargeValues()
//        {
//            Runtime runtime = Runtime.getRuntime();
//            double factor = 0.05;
//            double systemFactor = 2; // if I ask for a cache size of 10 MB, accept an overhead of 2x, i.e., 20 MBs might be allocated
//            long desiredCacheSize = Math.min(100 * 1024 * 1024L, runtime.maxMemory());
//            int keySizeBytes = 8;
//            int valueSizeBytes = 1000;

//            checkOverheads(factor, systemFactor, desiredCacheSize, keySizeBytes, valueSizeBytes);
//        }


//        static int MemoryCacheEntrySize(byte[] key, byte[] value, string topic)
//        {
//            return key.Length +
//                    value.Length +
//                    1 + // isDirty
//                    8 + // timestamp
//                    8 + // offset
//                    4 +
//                    topic.Length() +
//                    // LRU Node entries
//                    key.Length +
//                    8 + // entry
//                    8 + // previous
//                    8; // next
//        }

//        [Fact]
//        public void Evict()
//        {
//            List<KeyValuePair<string, string>> received = new List<KeyValuePair<string, string>>();
//            List<KeyValuePair<string, string>> expected = Collections.singletonList(
//                    KeyValuePair.Create("K1", "V1"));

//            List<KeyValuePair<string, string>> toInsert = Arrays.asList(
//                    KeyValuePair.Create("K1", "V1"),
//                    KeyValuePair.Create("K2", "V2"),
//                    KeyValuePair.Create("K3", "V3"),
//                    KeyValuePair.Create("K4", "V4"),
//                    KeyValuePair.Create("K5", "V5"));
//            KeyValuePair<string, string> kv = toInsert.Get(0);
//            ThreadCache cache = new ThreadCache(logContext,
//                                                      memoryCacheEntrySize(kv.Key.getBytes(), kv.Value.getBytes(), ""),
//                                                      new MockStreamsMetrics(new Metrics()));
//            cache.addDirtyEntryFlushListener(ns, dirty =>
//            {
//                foreach (ThreadCache.DirtyEntry dirtyEntry in dirty)
//                {
//                    received.Add(KeyValuePair.Create(dirtyEntry.Key.ToString(), new string(dirtyEntry.newValue())));
//                }
//            });

//            foreach (KeyValuePair<string, string> kvToInsert in toInsert)
//            {
//                Bytes key = Bytes.Wrap(kvToInsert.Key.getBytes());
//                byte[] value = kvToInsert.Value.getBytes();
//                cache.Put(ns, key, new LRUCacheEntry(value, null, true, 1, 1, 1, ""));
//            }

//            for (int i = 0; i < expected.Count; i++)
//            {
//                KeyValuePair<string, string> expectedRecord = expected.Get(i);
//                KeyValuePair<string, string> actualRecord = received.Get(i);
//                Assert.Equal(expectedRecord, actualRecord);
//            }
//            Assert.Equal(cache.evicts(), 4);
//        }

//        [Fact]
//        public void ShouldDelete()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
//            Bytes key = Bytes.Wrap(new byte[] { 0 });

//            cache.Put(ns, key, dirtyEntry(key.Get()));
//            Assert.Equal(key.Get(), cache.Delete(ns, key).Value);
//            Assert.Null(cache.Get(ns, key));
//        }

//        [Fact]
//        public void ShouldNotFlushAfterDelete()
//        {
//            Bytes key = Bytes.Wrap(new byte[] { 0 });
//            ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
//            List<ThreadCache.DirtyEntry> received = new List<ThreadCache.DirtyEntry>();
//            cache.addDirtyEntryFlushListener(ns, received::addAll);
//            cache.Put(ns, key, dirtyEntry(key.Get()));
//            Assert.Equal(key.Get(), cache.Delete(ns, key).Value);

//            // flushing should have no further effect
//            cache.Flush(ns);
//            Assert.Equal(0, received.Count);
//            Assert.Equal(cache.flushes(), 1);
//        }

//        [Fact]
//        public void ShouldNotBlowUpOnNonExistentKeyWhenDeleting()
//        {
//            Bytes key = Bytes.Wrap(new byte[] { 0 });
//            ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));

//            cache.Put(ns, key, dirtyEntry(key.Get()));
//            Assert.Null(cache.Delete(ns, Bytes.Wrap(new byte[] { 1 })));
//        }

//        [Fact]
//        public void ShouldNotBlowUpOnNonExistentNamespaceWhenDeleting()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
//            Assert.Null(cache.Delete(ns, Bytes.Wrap(new byte[] { 1 })));
//        }

//        [Fact]
//        public void ShouldNotClashWithOverlappingNames()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
//            Bytes nameByte = Bytes.Wrap(new byte[] { 0 });
//            Bytes name1Byte = Bytes.Wrap(new byte[] { 1 });
//            cache.Put(namespace1, nameByte, dirtyEntry(nameByte.Get()));
//            cache.Put(namespace2, nameByte, dirtyEntry(name1Byte.Get()));

//            assertArrayEquals(nameByte.Get(), cache.Get(namespace1, nameByte).Value);
//            assertArrayEquals(name1Byte.Get(), cache.Get(namespace2, nameByte).Value);
//        }

//        [Fact]
//        public void ShouldPeekNextKey()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
//            Bytes theByte = Bytes.Wrap(new byte[] { 0 });
//            cache.Put(ns, theByte, dirtyEntry(theByte.Get()));
//            ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.Range(ns, theByte, Bytes.Wrap(new byte[] { 1 }));
//            Assert.Equal(theByte, iterator.PeekNextKey());
//            Assert.Equal(theByte, iterator.PeekNextKey());
//        }

//        [Fact]
//        public void ShouldGetSameKeyAsPeekNext()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
//            Bytes theByte = Bytes.Wrap(new byte[] { 0 });
//            cache.Put(ns, theByte, dirtyEntry(theByte.Get()));
//            ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.Range(ns, theByte, Bytes.Wrap(new byte[] { 1 }));
//            Assert.Equal(iterator.PeekNextKey(), iterator.MoveNext().Key);
//        }

//        [Fact]// (expected = NoSuchElementException)
//        public void ShouldThrowIfNoPeekNextKey()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
//            ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.Range(ns, Bytes.Wrap(new byte[] { 0 }), Bytes.Wrap(new byte[] { 1 }));
//            iterator.PeekNextKey();
//        }

//        [Fact]
//        public void ShouldReturnFalseIfNoNextKey()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
//            ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.Range(ns, Bytes.Wrap(new byte[] { 0 }), Bytes.Wrap(new byte[] { 1 }));
//            Assert.False(iterator.MoveNext());
//        }

//        [Fact]
//        public void ShouldPeekAndIterateOverRange()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
//            byte[][] bytes = { { 0 }, { 1 }, { 2 }, { 3 }, { 4 }, { 5 }, { 6 }, { 7 }, { 8 }, { 9 }, { 10 } };
//            foreach (byte[] aByte in bytes)
//            {
//                cache.Put(ns, Bytes.Wrap(aByte), dirtyEntry(aByte));
//            }
//            ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.Range(ns, Bytes.Wrap(new byte[] { 1 }), Bytes.Wrap(new byte[] { 4 }));
//            int bytesIndex = 1;
//            while (iterator.MoveNext())
//            {
//                Bytes peekedKey = iterator.PeekNextKey();
//                KeyValuePair<Bytes, LRUCacheEntry> next = iterator.MoveNext();
//                assertArrayEquals(bytes[bytesIndex], peekedKey.Get());
//                assertArrayEquals(bytes[bytesIndex], next.Key.Get());
//                bytesIndex++;
//            }
//            Assert.Equal(5, bytesIndex);
//        }

//        [Fact]
//        public void ShouldSkipEntriesWhereValueHasBeenEvictedFromCache()
//        {
//            int entrySize = memoryCacheEntrySize(new byte[1], new byte[1], "");
//            ThreadCache cache = new ThreadCache(logContext, entrySize * 5, new MockStreamsMetrics(new Metrics()));
//            cache.addDirtyEntryFlushListener(ns, dirty => { });

//            byte[][] bytes = { { 0 }, { 1 }, { 2 }, { 3 }, { 4 }, { 5 }, { 6 }, { 7 }, { 8 }, { 9 } };
//            for (int i = 0; i < 5; i++)
//            {
//                cache.Put(ns, Bytes.Wrap(bytes[i]), dirtyEntry(bytes[i]));
//            }
//            Assert.Equal(5, cache.Count);

//            // should evict byte[] {0}
//            cache.Put(ns, Bytes.Wrap(new byte[] { 6 }), dirtyEntry(new byte[] { 6 }));

//            ThreadCache.MemoryLRUCacheBytesIterator range = cache.Range(ns, Bytes.Wrap(new byte[] { 0 }), Bytes.Wrap(new byte[] { 5 }));

//            Assert.Equal(Bytes.Wrap(new byte[] { 1 }), range.PeekNextKey());
//        }

//        [Fact]
//        public void ShouldFlushDirtyEntriesForNamespace()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
//            List<byte[]> received = new List<byte[]>();
//            cache.addDirtyEntryFlushListener(namespace1, dirty =>
//            {
//                foreach (ThreadCache.DirtyEntry dirtyEntry in dirty)
//                {
//                    received.Add(dirtyEntry.Key.Get());
//                }
//            });
//            List<byte[]> expected = Arrays.asList(new byte[] { 0 }, new byte[] { 1 }, new byte[] { 2 });
//            foreach (byte[] bytes in expected)
//            {
//                cache.Put(namespace1, Bytes.Wrap(bytes), dirtyEntry(bytes));
//            }
//            cache.Put(namespace2, Bytes.Wrap(new byte[] { 4 }), dirtyEntry(new byte[] { 4 }));

//            cache.Flush(namespace1);
//            Assert.Equal(expected, received);
//        }

//        [Fact]
//        public void ShouldNotFlushCleanEntriesForNamespace()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
//            List<byte[]> received = new List<byte[]>();
//            cache.addDirtyEntryFlushListener(namespace1, dirty =>
//            {
//                foreach (ThreadCache.DirtyEntry dirtyEntry in dirty)
//                {
//                    received.Add(dirtyEntry.Key.Get());
//                }
//            });
//            List<byte[]> toInsert = Arrays.asList(new byte[] { 0 }, new byte[] { 1 }, new byte[] { 2 });
//            foreach (byte[] bytes in toInsert)
//            {
//                cache.Put(namespace1, Bytes.Wrap(bytes), cleanEntry(bytes));
//            }
//            cache.Put(namespace2, Bytes.Wrap(new byte[] { 4 }), cleanEntry(new byte[] { 4 }));

//            cache.Flush(namespace1);
//            Assert.Equal(Collections.emptyList(), received);
//        }


//        private void ShouldEvictImmediatelyIfCacheSizeIsZeroOrVerySmall(ThreadCache cache)
//        {
//            List<ThreadCache.DirtyEntry> received = new List<ThreadCache.DirtyEntry>();

//            cache.addDirtyEntryFlushListener(ns, received::addAll);
//            cache.Put(ns, Bytes.Wrap(new byte[] { 0 }), dirtyEntry(new byte[] { 0 }));
//            Assert.Equal(1, received.Count);

//            // flushing should have no further effect
//            cache.Flush(ns);
//            Assert.Equal(1, received.Count);
//        }

//        [Fact]
//        public void ShouldEvictImmediatelyIfCacheSizeIsVerySmall()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 1, new MockStreamsMetrics(new Metrics()));
//            shouldEvictImmediatelyIfCacheSizeIsZeroOrVerySmall(cache);
//        }

//        [Fact]
//        public void ShouldEvictImmediatelyIfCacheSizeIsZero()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 0, new MockStreamsMetrics(new Metrics()));
//            shouldEvictImmediatelyIfCacheSizeIsZeroOrVerySmall(cache);
//        }

//        [Fact]
//        public void ShouldEvictAfterPutAll()
//        {
//            List<ThreadCache.DirtyEntry> received = new List<ThreadCache.DirtyEntry>();
//            ThreadCache cache = new ThreadCache(logContext, 1, new MockStreamsMetrics(new Metrics()));
//            cache.addDirtyEntryFlushListener(ns, received::addAll);

//            cache.PutAll(ns, Arrays.asList(KeyValuePair.Create(Bytes.Wrap(new byte[] { 0 }), dirtyEntry(new byte[] { 5 })),
//                                                  KeyValuePair.Create(Bytes.Wrap(new byte[] { 1 }), dirtyEntry(new byte[] { 6 }))));

//            Assert.Equal(cache.evicts(), 2);
//            Assert.Equal(received.Count, 2);
//        }

//        [Fact]
//        public void ShouldPutAll()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));

//            cache.PutAll(ns, Arrays.asList(KeyValuePair.Create(Bytes.Wrap(new byte[] { 0 }), dirtyEntry(new byte[] { 5 })),
//                                                   KeyValuePair.Create(Bytes.Wrap(new byte[] { 1 }), dirtyEntry(new byte[] { 6 }))));

//            assertArrayEquals(new byte[] { 5 }, cache.Get(ns, Bytes.Wrap(new byte[] { 0 })).Value);
//            assertArrayEquals(new byte[] { 6 }, cache.Get(ns, Bytes.Wrap(new byte[] { 1 })).Value);
//        }

//        [Fact]
//        public void ShouldNotForwardCleanEntryOnEviction()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 0, new MockStreamsMetrics(new Metrics()));
//            List<ThreadCache.DirtyEntry> received = new List<ThreadCache.DirtyEntry>();
//            cache.addDirtyEntryFlushListener(ns, received::addAll);
//            cache.Put(ns, Bytes.Wrap(new byte[] { 1 }), cleanEntry(new byte[] { 0 }));
//            Assert.Equal(0, received.Count);
//        }
//        [Fact]
//        public void ShouldPutIfAbsent()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
//            Bytes key = Bytes.Wrap(new byte[] { 10 });
//            byte[] value = { 30 };
//            Assert.Null(cache.PutIfAbsent(ns, key, dirtyEntry(value)));
//            assertArrayEquals(value, cache.PutIfAbsent(ns, key, dirtyEntry(new byte[] { 8 })).Value);
//            assertArrayEquals(value, cache.Get(ns, key).Value);
//        }

//        [Fact]
//        public void ShouldEvictAfterPutIfAbsent()
//        {
//            List<ThreadCache.DirtyEntry> received = new List<ThreadCache.DirtyEntry>();
//            ThreadCache cache = new ThreadCache(logContext, 1, new MockStreamsMetrics(new Metrics()));
//            cache.addDirtyEntryFlushListener(ns, received::addAll);

//            cache.PutIfAbsent(ns, Bytes.Wrap(new byte[] { 0 }), dirtyEntry(new byte[] { 5 }));
//            cache.PutIfAbsent(ns, Bytes.Wrap(new byte[] { 1 }), dirtyEntry(new byte[] { 6 }));
//            cache.PutIfAbsent(ns, Bytes.Wrap(new byte[] { 1 }), dirtyEntry(new byte[] { 6 }));

//            Assert.Equal(cache.evicts(), 3);
//            Assert.Equal(received.Count, 3);
//        }

//        [Fact]
//        public void ShouldNotLoopForEverWhenEvictingAndCurrentCacheIsEmpty()
//        {
//            int maxCacheSizeInBytes = 100;
//            ThreadCache threadCache = new ThreadCache(logContext, maxCacheSizeInBytes, new MockStreamsMetrics(new Metrics()));
//            // trigger a Put into another cache on eviction from "Name"
//            threadCache.addDirtyEntryFlushListener(ns, dirty =>
//            {
//                // Put an item into an empty cache when the total cache size
//                // is already > than maxCacheSizeBytes
//                threadCache.Put(namespace1, Bytes.Wrap(new byte[] { 0 }), dirtyEntry(new byte[2]));
//            });
//            threadCache.addDirtyEntryFlushListener(namespace1, dirty => { });
//            threadCache.addDirtyEntryFlushListener(namespace2, dirty => { });

//            threadCache.Put(namespace2, Bytes.Wrap(new byte[] { 1 }), dirtyEntry(new byte[1]));
//            threadCache.Put(ns, Bytes.Wrap(new byte[] { 1 }), dirtyEntry(new byte[1]));
//            // Put a large item such that when the eldest item is removed
//            // cache sizeInBytes() > maxCacheSizeBytes
//            int remaining = (int)(maxCacheSizeInBytes - threadCache.sizeBytes());
//            threadCache.Put(ns, Bytes.Wrap(new byte[] { 2 }), dirtyEntry(new byte[remaining + 100]));
//        }

//        [Fact]
//        public void ShouldCleanupNamedCacheOnClose()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
//            cache.Put(namespace1, Bytes.Wrap(new byte[] { 1 }), cleanEntry(new byte[] { 1 }));
//            cache.Put(namespace2, Bytes.Wrap(new byte[] { 1 }), cleanEntry(new byte[] { 1 }));
//            Assert.Equal(cache.Count, 2);
//            cache.Close(namespace2);
//            Assert.Equal(cache.Count, 1);
//            Assert.Null(cache.Get(namespace2, Bytes.Wrap(new byte[] { 1 })));
//        }

//        [Fact]
//        public void ShouldReturnNullIfKeyIsNull()
//        {
//            ThreadCache threadCache = new ThreadCache(logContext, 10, new MockStreamsMetrics(new Metrics()));
//            threadCache.Put(ns, Bytes.Wrap(new byte[] { 1 }), cleanEntry(new byte[] { 1 }));
//            Assert.Null(threadCache.Get(ns, null));
//        }

//        [Fact]
//        public void ShouldCalculateSizeInBytes()
//        {
//            ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
//            NamedCache.LRUNode node = new NamedCache.LRUNode(Bytes.Wrap(new byte[] { 1 }), dirtyEntry(new byte[] { 0 }));
//            cache.Put(namespace1, Bytes.Wrap(new byte[] { 1 }), cleanEntry(new byte[] { 0 }));
//            Assert.Equal(cache.sizeBytes(), node.Count);
//        }

//        private LRUCacheEntry DirtyEntry(byte[] key)
//        {
//            return new LRUCacheEntry(key, null, true, -1, -1, -1, "");
//        }

//        private LRUCacheEntry CleanEntry(byte[] key)
//        {
//            return new LRUCacheEntry(key);
//        }
//    }
//}
