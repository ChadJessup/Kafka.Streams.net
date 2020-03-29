/*






 *

 *





 */























public class ThreadCacheTest {
    string namespace = "0.0-namespace";
    string namespace1 = "0.1-namespace";
    string namespace2 = "0.2-namespace";
    private LogContext logContext = new LogContext("testCache ");

    [Xunit.Fact]
    public void BasicPutGet(){ //throws IOException
        List<KeyValuePair<string, string>> toInsert = Array.asList(
                new KeyValuePair<>("K1", "V1"),
                new KeyValuePair<>("K2", "V2"),
                new KeyValuePair<>("K3", "V3"),
                new KeyValuePair<>("K4", "V4"),
                new KeyValuePair<>("K5", "V5"));
        KeyValuePair<string, string> kv = toInsert.get(0);
        ThreadCache cache = new ThreadCache(logContext,
                                                  toInsert.Count * memoryCacheEntrySize(kv.key.getBytes(), kv.value.getBytes(), ""),
                                                  new MockStreamsMetrics(new Metrics()));

        foreach (KeyValuePair<string, string> kvToInsert in toInsert) {
            Bytes key = Bytes.wrap(kvToInsert.key.getBytes());
            byte[] value = kvToInsert.value.getBytes();
            cache.put(namespace, key, new LRUCacheEntry(value, null, true, 1L, 1L, 1, ""));
        }

        foreach (KeyValuePair<string, string> kvToInsert in toInsert) {
            Bytes key = Bytes.wrap(kvToInsert.key.getBytes());
            LRUCacheEntry entry = cache.get(namespace, key);
            Assert.Equal(entry.isDirty(), true);
            Assert.Equal(new string(entry.Value), kvToInsert.value);
        }
        Assert.Equal(cache.gets(), 5);
        Assert.Equal(cache.puts(), 5);
        Assert.Equal(cache.evicts(), 0);
        Assert.Equal(cache.flushes(), 0);
    }

    private void CheckOverheads(double entryFactor,
                                double systemFactor,
                                long desiredCacheSize,
                                int keySizeBytes,
                                int valueSizeBytes) {
        Runtime runtime = Runtime.getRuntime();
        long numElements = desiredCacheSize / memoryCacheEntrySize(new byte[keySizeBytes], new byte[valueSizeBytes], "");

        System.gc();
        long prevRuntimeMemory = runtime.totalMemory() - runtime.freeMemory();

        ThreadCache cache = new ThreadCache(logContext, desiredCacheSize, new MockStreamsMetrics(new Metrics()));
        long size = cache.sizeBytes();
        Assert.Equal(size, 0);
        for (int i = 0; i < numElements; i++) {
            string keyStr = "K" + i;
            Bytes key = Bytes.wrap(keyStr.getBytes());
            byte[] value = new byte[valueSizeBytes];
            cache.put(namespace, key, new LRUCacheEntry(value, null, true, 1L, 1L, 1, ""));
        }


        System.gc();
        double ceiling = desiredCacheSize + desiredCacheSize * entryFactor;
        long usedRuntimeMemory = runtime.totalMemory() - runtime.freeMemory() - prevRuntimeMemory;
        Assert.True((double) cache.sizeBytes() <= ceiling);

        Assert.True("Used memory size " + usedRuntimeMemory + " greater than expected " + cache.sizeBytes() * systemFactor,
            cache.sizeBytes() * systemFactor >= usedRuntimeMemory);
    }

    [Xunit.Fact]
    public void CacheOverheadsSmallValues() {
        Runtime runtime = Runtime.getRuntime();
        double factor = 0.05;
        double systemFactor = 3; // if I ask for a cache size of 10 MB, accept an overhead of 3x, i.e., 30 MBs might be allocated
        long desiredCacheSize = Math.min(100 * 1024 * 1024L, runtime.maxMemory());
        int keySizeBytes = 8;
        int valueSizeBytes = 100;

        checkOverheads(factor, systemFactor, desiredCacheSize, keySizeBytes, valueSizeBytes);
    }

    [Xunit.Fact]
    public void CacheOverheadsLargeValues() {
        Runtime runtime = Runtime.getRuntime();
        double factor = 0.05;
        double systemFactor = 2; // if I ask for a cache size of 10 MB, accept an overhead of 2x, i.e., 20 MBs might be allocated
        long desiredCacheSize = Math.min(100 * 1024 * 1024L, runtime.maxMemory());
        int keySizeBytes = 8;
        int valueSizeBytes = 1000;

        checkOverheads(factor, systemFactor, desiredCacheSize, keySizeBytes, valueSizeBytes);
    }


    static int MemoryCacheEntrySize(byte[] key, byte[] value, string topic) {
        return key.Length +
                value.Length +
                1 + // isDirty
                8 + // timestamp
                8 + // offset
                4 +
                topic.Length() +
                // LRU Node entries
                key.Length +
                8 + // entry
                8 + // previous
                8; // next
    }

    [Xunit.Fact]
    public void Evict() {
        List<KeyValuePair<string, string>> received = new ArrayList<>();
        List<KeyValuePair<string, string>> expected = Collections.singletonList(
                new KeyValuePair<>("K1", "V1"));

        List<KeyValuePair<string, string>> toInsert = Array.asList(
                new KeyValuePair<>("K1", "V1"),
                new KeyValuePair<>("K2", "V2"),
                new KeyValuePair<>("K3", "V3"),
                new KeyValuePair<>("K4", "V4"),
                new KeyValuePair<>("K5", "V5"));
        KeyValuePair<string, string> kv = toInsert.get(0);
        ThreadCache cache = new ThreadCache(logContext,
                                                  memoryCacheEntrySize(kv.key.getBytes(), kv.value.getBytes(), ""),
                                                  new MockStreamsMetrics(new Metrics()));
        cache.addDirtyEntryFlushListener(namespace, dirty => {
            foreach (ThreadCache.DirtyEntry dirtyEntry in dirty) {
                received.add(new KeyValuePair<>(dirtyEntry.Key.toString(), new string(dirtyEntry.newValue())));
            }
        });

        foreach (KeyValuePair<string, string> kvToInsert in toInsert) {
            Bytes key = Bytes.wrap(kvToInsert.key.getBytes());
            byte[] value = kvToInsert.value.getBytes();
            cache.put(namespace, key, new LRUCacheEntry(value, null, true, 1, 1, 1, ""));
        }

        for (int i = 0; i < expected.Count; i++) {
            KeyValuePair<string, string> expectedRecord = expected.get(i);
            KeyValuePair<string, string> actualRecord = received.get(i);
            Assert.Equal(expectedRecord, actualRecord);
        }
        Assert.Equal(cache.evicts(), 4);
    }

    [Xunit.Fact]
    public void ShouldDelete() {
        ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        Bytes key = Bytes.wrap(new byte[]{0});

        cache.put(namespace, key, dirtyEntry(key.get()));
        Assert.Equal(key.get(), cache.delete(namespace, key).Value);
        assertNull(cache.get(namespace, key));
    }

    [Xunit.Fact]
    public void ShouldNotFlushAfterDelete() {
        Bytes key = Bytes.wrap(new byte[]{0});
        ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        List<ThreadCache.DirtyEntry> received = new ArrayList<>();
        cache.addDirtyEntryFlushListener(namespace, received::addAll);
        cache.put(namespace, key, dirtyEntry(key.get()));
        Assert.Equal(key.get(), cache.delete(namespace, key).Value);

        // flushing should have no further effect
        cache.flush(namespace);
        Assert.Equal(0, received.Count);
        Assert.Equal(cache.flushes(), 1);
    }

    [Xunit.Fact]
    public void ShouldNotBlowUpOnNonExistentKeyWhenDeleting() {
        Bytes key = Bytes.wrap(new byte[]{0});
        ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));

        cache.put(namespace, key, dirtyEntry(key.get()));
        assertNull(cache.delete(namespace, Bytes.wrap(new byte[]{1})));
    }

    [Xunit.Fact]
    public void ShouldNotBlowUpOnNonExistentNamespaceWhenDeleting() {
        ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        assertNull(cache.delete(namespace, Bytes.wrap(new byte[]{1})));
    }

    [Xunit.Fact]
    public void ShouldNotClashWithOverlappingNames() {
        ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        Bytes nameByte = Bytes.wrap(new byte[]{0});
        Bytes name1Byte = Bytes.wrap(new byte[]{1});
        cache.put(namespace1, nameByte, dirtyEntry(nameByte.get()));
        cache.put(namespace2, nameByte, dirtyEntry(name1Byte.get()));

        assertArrayEquals(nameByte.get(), cache.get(namespace1, nameByte).Value);
        assertArrayEquals(name1Byte.get(), cache.get(namespace2, nameByte).Value);
    }

    [Xunit.Fact]
    public void ShouldPeekNextKey() {
        ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        Bytes theByte = Bytes.wrap(new byte[]{0});
        cache.put(namespace, theByte, dirtyEntry(theByte.get()));
        ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, theByte, Bytes.wrap(new byte[]{1}));
        Assert.Equal(theByte, iterator.peekNextKey());
        Assert.Equal(theByte, iterator.peekNextKey());
    }

    [Xunit.Fact]
    public void ShouldGetSameKeyAsPeekNext() {
        ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        Bytes theByte = Bytes.wrap(new byte[]{0});
        cache.put(namespace, theByte, dirtyEntry(theByte.get()));
        ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, theByte, Bytes.wrap(new byte[]{1}));
        Assert.Equal(iterator.peekNextKey(), iterator.next().key);
    }

    [Xunit.Fact]// (expected = NoSuchElementException)
    public void ShouldThrowIfNoPeekNextKey() {
        ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, Bytes.wrap(new byte[]{0}), Bytes.wrap(new byte[]{1}));
        iterator.peekNextKey();
    }

    [Xunit.Fact]
    public void ShouldReturnFalseIfNoNextKey() {
        ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, Bytes.wrap(new byte[]{0}), Bytes.wrap(new byte[]{1}));
        Assert.False(iterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldPeekAndIterateOverRange() {
        ThreadCache cache = new ThreadCache(logContext, 10000L, new MockStreamsMetrics(new Metrics()));
        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}};
        foreach (byte[] aByte in bytes) {
            cache.put(namespace, Bytes.wrap(aByte), dirtyEntry(aByte));
        }
        ThreadCache.MemoryLRUCacheBytesIterator iterator = cache.range(namespace, Bytes.wrap(new byte[]{1}), Bytes.wrap(new byte[]{4}));
        int bytesIndex = 1;
        while (iterator.hasNext()) {
            Bytes peekedKey = iterator.peekNextKey();
            KeyValuePair<Bytes, LRUCacheEntry> next = iterator.next();
            assertArrayEquals(bytes[bytesIndex], peekedKey.get());
            assertArrayEquals(bytes[bytesIndex], next.key.get());
            bytesIndex++;
        }
        Assert.Equal(5, bytesIndex);
    }

    [Xunit.Fact]
    public void ShouldSkipEntriesWhereValueHasBeenEvictedFromCache() {
        int entrySize = memoryCacheEntrySize(new byte[1], new byte[1], "");
        ThreadCache cache = new ThreadCache(logContext, entrySize * 5, new MockStreamsMetrics(new Metrics()));
        cache.addDirtyEntryFlushListener(namespace, dirty => { });

        byte[][] bytes = {{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}};
        for (int i = 0; i < 5; i++) {
            cache.put(namespace, Bytes.wrap(bytes[i]), dirtyEntry(bytes[i]));
        }
        Assert.Equal(5, cache.Count);

        // should evict byte[] {0}
        cache.put(namespace, Bytes.wrap(new byte[]{6}), dirtyEntry(new byte[]{6}));

        ThreadCache.MemoryLRUCacheBytesIterator range = cache.range(namespace, Bytes.wrap(new byte[]{0}), Bytes.wrap(new byte[]{5}));

        Assert.Equal(Bytes.wrap(new byte[]{1}), range.peekNextKey());
    }

    [Xunit.Fact]
    public void ShouldFlushDirtyEntriesForNamespace() {
        ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
        List<byte[]> received = new ArrayList<>();
        cache.addDirtyEntryFlushListener(namespace1, dirty => {
            foreach (ThreadCache.DirtyEntry dirtyEntry in dirty) {
                received.add(dirtyEntry.Key.get());
            }
        });
        List<byte[]> expected = Array.asList(new byte[]{0}, new byte[]{1}, new byte[]{2});
        foreach (byte[] bytes in expected) {
            cache.put(namespace1, Bytes.wrap(bytes), dirtyEntry(bytes));
        }
        cache.put(namespace2, Bytes.wrap(new byte[]{4}), dirtyEntry(new byte[]{4}));

        cache.flush(namespace1);
        Assert.Equal(expected, received);
    }

    [Xunit.Fact]
    public void ShouldNotFlushCleanEntriesForNamespace() {
        ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
        List<byte[]> received = new ArrayList<>();
        cache.addDirtyEntryFlushListener(namespace1, dirty => {
            foreach (ThreadCache.DirtyEntry dirtyEntry in dirty) {
                received.add(dirtyEntry.Key.get());
            }
        });
        List<byte[]> toInsert =  Array.asList(new byte[]{0}, new byte[]{1}, new byte[]{2});
        foreach (byte[] bytes in toInsert) {
            cache.put(namespace1, Bytes.wrap(bytes), cleanEntry(bytes));
        }
        cache.put(namespace2, Bytes.wrap(new byte[]{4}), cleanEntry(new byte[]{4}));

        cache.flush(namespace1);
        Assert.Equal(Collections.emptyList(), received);
    }


    private void ShouldEvictImmediatelyIfCacheSizeIsZeroOrVerySmall(ThreadCache cache) {
        List<ThreadCache.DirtyEntry> received = new ArrayList<>();

        cache.addDirtyEntryFlushListener(namespace, received::addAll);
        cache.put(namespace, Bytes.wrap(new byte[]{0}), dirtyEntry(new byte[]{0}));
        Assert.Equal(1, received.Count);

        // flushing should have no further effect
        cache.flush(namespace);
        Assert.Equal(1, received.Count);
    }

    [Xunit.Fact]
    public void ShouldEvictImmediatelyIfCacheSizeIsVerySmall() {
        ThreadCache cache = new ThreadCache(logContext, 1, new MockStreamsMetrics(new Metrics()));
        shouldEvictImmediatelyIfCacheSizeIsZeroOrVerySmall(cache);
    }

    [Xunit.Fact]
    public void ShouldEvictImmediatelyIfCacheSizeIsZero() {
        ThreadCache cache = new ThreadCache(logContext, 0, new MockStreamsMetrics(new Metrics()));
        shouldEvictImmediatelyIfCacheSizeIsZeroOrVerySmall(cache);
    }

    [Xunit.Fact]
    public void ShouldEvictAfterPutAll() {
        List<ThreadCache.DirtyEntry> received = new ArrayList<>();
        ThreadCache cache = new ThreadCache(logContext, 1, new MockStreamsMetrics(new Metrics()));
        cache.addDirtyEntryFlushListener(namespace, received::addAll);

        cache.putAll(namespace, Array.asList(KeyValuePair.Create(Bytes.wrap(new byte[]{0}), dirtyEntry(new byte[]{5})),
                                              KeyValuePair.Create(Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[]{6}))));

        Assert.Equal(cache.evicts(), 2);
        Assert.Equal(received.Count, 2);
    }

    [Xunit.Fact]
    public void ShouldPutAll() {
        ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));

        cache.putAll(namespace, Array.asList(KeyValuePair.Create(Bytes.wrap(new byte[]{0}), dirtyEntry(new byte[]{5})),
                                           KeyValuePair.Create(Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[]{6}))));

        assertArrayEquals(new byte[]{5}, cache.get(namespace, Bytes.wrap(new byte[]{0})).Value);
        assertArrayEquals(new byte[]{6}, cache.get(namespace, Bytes.wrap(new byte[]{1})).Value);
    }

    [Xunit.Fact]
    public void ShouldNotForwardCleanEntryOnEviction() {
        ThreadCache cache = new ThreadCache(logContext, 0, new MockStreamsMetrics(new Metrics()));
        List<ThreadCache.DirtyEntry> received = new ArrayList<>();
        cache.addDirtyEntryFlushListener(namespace, received::addAll);
        cache.put(namespace, Bytes.wrap(new byte[]{1}), cleanEntry(new byte[]{0}));
        Assert.Equal(0, received.Count);
    }
    [Xunit.Fact]
    public void ShouldPutIfAbsent() {
        ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
        Bytes key = Bytes.wrap(new byte[]{10});
        byte[] value = {30};
        assertNull(cache.putIfAbsent(namespace, key, dirtyEntry(value)));
        assertArrayEquals(value, cache.putIfAbsent(namespace, key, dirtyEntry(new byte[]{8})).Value);
        assertArrayEquals(value, cache.get(namespace, key).Value);
    }

    [Xunit.Fact]
    public void ShouldEvictAfterPutIfAbsent() {
        List<ThreadCache.DirtyEntry> received = new ArrayList<>();
        ThreadCache cache = new ThreadCache(logContext, 1, new MockStreamsMetrics(new Metrics()));
        cache.addDirtyEntryFlushListener(namespace, received::addAll);

        cache.putIfAbsent(namespace, Bytes.wrap(new byte[]{0}), dirtyEntry(new byte[]{5}));
        cache.putIfAbsent(namespace, Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[]{6}));
        cache.putIfAbsent(namespace, Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[]{6}));

        Assert.Equal(cache.evicts(), 3);
        Assert.Equal(received.Count, 3);
    }

    [Xunit.Fact]
    public void ShouldNotLoopForEverWhenEvictingAndCurrentCacheIsEmpty() {
        int maxCacheSizeInBytes = 100;
        ThreadCache threadCache = new ThreadCache(logContext, maxCacheSizeInBytes, new MockStreamsMetrics(new Metrics()));
        // trigger a put into another cache on eviction from "name"
        threadCache.addDirtyEntryFlushListener(namespace, dirty => {
            // put an item into an empty cache when the total cache size
            // is already > than maxCacheSizeBytes
            threadCache.put(namespace1, Bytes.wrap(new byte[]{0}), dirtyEntry(new byte[2]));
        });
        threadCache.addDirtyEntryFlushListener(namespace1, dirty => { });
        threadCache.addDirtyEntryFlushListener(namespace2, dirty => { });

        threadCache.put(namespace2, Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[1]));
        threadCache.put(namespace, Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[1]));
        // Put a large item such that when the eldest item is removed
        // cache sizeInBytes() > maxCacheSizeBytes
        int remaining = (int) (maxCacheSizeInBytes - threadCache.sizeBytes());
        threadCache.put(namespace, Bytes.wrap(new byte[]{2}), dirtyEntry(new byte[remaining + 100]));
    }

    [Xunit.Fact]
    public void ShouldCleanupNamedCacheOnClose() {
        ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
        cache.put(namespace1, Bytes.wrap(new byte[]{1}), cleanEntry(new byte[] {1}));
        cache.put(namespace2, Bytes.wrap(new byte[]{1}), cleanEntry(new byte[] {1}));
        Assert.Equal(cache.Count, 2);
        cache.close(namespace2);
        Assert.Equal(cache.Count, 1);
        assertNull(cache.get(namespace2, Bytes.wrap(new byte[]{1})));
    }

    [Xunit.Fact]
    public void ShouldReturnNullIfKeyIsNull() {
        ThreadCache threadCache = new ThreadCache(logContext, 10, new MockStreamsMetrics(new Metrics()));
        threadCache.put(namespace, Bytes.wrap(new byte[]{1}), cleanEntry(new byte[] {1}));
        assertNull(threadCache.get(namespace, null));
    }

    [Xunit.Fact]
    public void ShouldCalculateSizeInBytes() {
        ThreadCache cache = new ThreadCache(logContext, 100000, new MockStreamsMetrics(new Metrics()));
        NamedCache.LRUNode node = new NamedCache.LRUNode(Bytes.wrap(new byte[]{1}), dirtyEntry(new byte[]{0}));
        cache.put(namespace1, Bytes.wrap(new byte[]{1}), cleanEntry(new byte[]{0}));
        Assert.Equal(cache.sizeBytes(), node.Count);
    }

    private LRUCacheEntry DirtyEntry(byte[] key) {
        return new LRUCacheEntry(key, null, true, -1, -1, -1, "");
    }

    private LRUCacheEntry CleanEntry(byte[] key) {
        return new LRUCacheEntry(key);
    }


}
