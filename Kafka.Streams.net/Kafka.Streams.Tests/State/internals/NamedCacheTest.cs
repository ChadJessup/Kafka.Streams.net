/*






 *

 *





 */






























public class NamedCacheTest {

    private Headers headers = new Headers(new Header[]{new RecordHeader("key", "value".getBytes())});
    private NamedCache cache;
    private Metrics innerMetrics;
    private StreamsMetricsImpl metrics;
    private readonly string taskIDString = "0.0";
    private readonly string underlyingStoreName = "storeName";

    
    public void SetUp() {
        innerMetrics = new Metrics();
        metrics = new MockStreamsMetrics(innerMetrics);
        cache = new NamedCache(taskIDString + "-" + underlyingStoreName, metrics);
    }

    [Xunit.Fact]
    public void ShouldKeepTrackOfMostRecentlyAndLeastRecentlyUsed(){ //throws IOException
        List<KeyValuePair<string, string>> toInsert = Array.asList(
                new KeyValuePair<>("K1", "V1"),
                new KeyValuePair<>("K2", "V2"),
                new KeyValuePair<>("K3", "V3"),
                new KeyValuePair<>("K4", "V4"),
                new KeyValuePair<>("K5", "V5"));
        for (int i = 0; i < toInsert.Count; i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            byte[] value = toInsert.get(i).value.getBytes();
            cache.put(Bytes.wrap(key), new LRUCacheEntry(value, null, true, 1, 1, 1, ""));
            LRUCacheEntry head = cache.first();
            LRUCacheEntry tail = cache.last();
            Assert.Equal(new string(head.Value), toInsert.get(i).value);
            Assert.Equal(new string(tail.Value), toInsert.get(0).value);
            Assert.Equal(cache.flushes(), 0);
            Assert.Equal(cache.hits(), 0);
            Assert.Equal(cache.misses(), 0);
            Assert.Equal(cache.overwrites(), 0);
        }
    }

    [Xunit.Fact]
    public void TestMetrics() {
        Dictionary<string, string> metricTags = new LinkedHashMap<>();
        metricTags.put("record-cache-id", underlyingStoreName);
        metricTags.put("task-id", taskIDString);
        metricTags.put("client-id", "test");

        getMetricByNameFilterByTags(metrics.metrics(), "hitRatio-avg", "stream-record-cache-metrics", metricTags);
        getMetricByNameFilterByTags(metrics.metrics(), "hitRatio-min", "stream-record-cache-metrics", metricTags);
        getMetricByNameFilterByTags(metrics.metrics(), "hitRatio-max", "stream-record-cache-metrics", metricTags);

        // test "all"
        metricTags.put("record-cache-id", "all");
        getMetricByNameFilterByTags(metrics.metrics(), "hitRatio-avg", "stream-record-cache-metrics", metricTags);
        getMetricByNameFilterByTags(metrics.metrics(), "hitRatio-min", "stream-record-cache-metrics", metricTags);
        getMetricByNameFilterByTags(metrics.metrics(), "hitRatio-max", "stream-record-cache-metrics", metricTags);

        JmxReporter reporter = new JmxReporter("kafka.streams");
        innerMetrics.addReporter(reporter);
        Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-record-cache-metrics,client-id=test,task-id=%s,record-cache-id=%s",
                taskIDString, underlyingStoreName)));
        Assert.True(reporter.containsMbean(string.format("kafka.streams:type=stream-record-cache-metrics,client-id=test,task-id=%s,record-cache-id=%s",
                taskIDString, "all")));
    }

    [Xunit.Fact]
    public void ShouldKeepTrackOfSize() {
        LRUCacheEntry value = new LRUCacheEntry(new byte[]{0});
        cache.put(Bytes.wrap(new byte[]{0}), value);
        cache.put(Bytes.wrap(new byte[]{1}), value);
        cache.put(Bytes.wrap(new byte[]{2}), value);
        long size = cache.sizeInBytes();
        // 1 byte key + 24 bytes overhead
        Assert.Equal((value.Count + 25) * 3, size);
    }

    [Xunit.Fact]
    public void ShouldPutGet() {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{11}));
        cache.put(Bytes.wrap(new byte[]{2}), new LRUCacheEntry(new byte[]{12}));

        assertArrayEquals(new byte[] {10}, cache.get(Bytes.wrap(new byte[] {0})).Value);
        assertArrayEquals(new byte[] {11}, cache.get(Bytes.wrap(new byte[] {1})).Value);
        assertArrayEquals(new byte[] {12}, cache.get(Bytes.wrap(new byte[] {2})).Value);
        Assert.Equal(cache.hits(), 3);
    }

    [Xunit.Fact]
    public void ShouldPutIfAbsent() {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}));
        cache.putIfAbsent(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{20}));
        cache.putIfAbsent(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{30}));

        assertArrayEquals(new byte[] {10}, cache.get(Bytes.wrap(new byte[] {0})).Value);
        assertArrayEquals(new byte[] {30}, cache.get(Bytes.wrap(new byte[] {1})).Value);
    }

    [Xunit.Fact]
    public void ShouldDeleteAndUpdateSize() {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}));
        LRUCacheEntry deleted = cache.delete(Bytes.wrap(new byte[]{0}));
        assertArrayEquals(new byte[] {10}, deleted.Value);
        Assert.Equal(0, cache.sizeInBytes());
    }

    [Xunit.Fact]
    public void ShouldPutAll() {
        cache.putAll(Array.asList(KeyValuePair.Create(new byte[] {0}, new LRUCacheEntry(new byte[]{0})),
                                   KeyValuePair.Create(new byte[] {1}, new LRUCacheEntry(new byte[]{1})),
                                   KeyValuePair.Create(new byte[] {2}, new LRUCacheEntry(new byte[]{2}))));

        assertArrayEquals(new byte[]{0}, cache.get(Bytes.wrap(new byte[]{0})).Value);
        assertArrayEquals(new byte[]{1}, cache.get(Bytes.wrap(new byte[]{1})).Value);
        assertArrayEquals(new byte[]{2}, cache.get(Bytes.wrap(new byte[]{2})).Value);
    }

    [Xunit.Fact]
    public void ShouldOverwriteAll() {
        cache.putAll(Array.asList(KeyValuePair.Create(new byte[] {0}, new LRUCacheEntry(new byte[]{0})),
            KeyValuePair.Create(new byte[] {0}, new LRUCacheEntry(new byte[]{1})),
            KeyValuePair.Create(new byte[] {0}, new LRUCacheEntry(new byte[]{2}))));

        assertArrayEquals(new byte[]{2}, cache.get(Bytes.wrap(new byte[]{0})).Value);
        Assert.Equal(cache.overwrites(), 2);
    }

    [Xunit.Fact]
    public void ShouldEvictEldestEntry() {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{20}));
        cache.put(Bytes.wrap(new byte[]{2}), new LRUCacheEntry(new byte[]{30}));

        cache.evict();
        assertNull(cache.get(Bytes.wrap(new byte[]{0})));
        Assert.Equal(2, cache.Count);
    }

    [Xunit.Fact]
    public void ShouldFlushDirtEntriesOnEviction() {
        List<ThreadCache.DirtyEntry> flushed = new ArrayList<>();
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}, headers, true, 0, 0, 0, ""));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{20}));
        cache.put(Bytes.wrap(new byte[]{2}), new LRUCacheEntry(new byte[]{30}, headers, true, 0, 0, 0, ""));

        cache.setListener(new ThreadCache.DirtyEntryFlushListener() {
            
            public void apply(List<ThreadCache.DirtyEntry> dirty) {
                flushed.addAll(dirty);
            }
        });

        cache.evict();

        Assert.Equal(2, flushed.Count);
        Assert.Equal(Bytes.wrap(new byte[] {0}), flushed.get(0).Key);
        Assert.Equal(headers, flushed.get(0).entry().context().headers());
        assertArrayEquals(new byte[] {10}, flushed.get(0).newValue());
        Assert.Equal(Bytes.wrap(new byte[] {2}), flushed.get(1).Key);
        assertArrayEquals(new byte[] {30}, flushed.get(1).newValue());
        Assert.Equal(cache.flushes(), 1);
    }

    [Xunit.Fact]
    public void ShouldNotThrowNullPointerWhenCacheIsEmptyAndEvictionCalled() {
        cache.evict();
    }

    [Xunit.Fact]// (expected = IllegalStateException)
    public void ShouldThrowIllegalStateExceptionWhenTryingToOverwriteDirtyEntryWithCleanEntry() {
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}, headers, true, 0, 0, 0, ""));
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(new byte[]{10}, null, false, 0, 0, 0, ""));
    }

    [Xunit.Fact]
    public void ShouldRemoveDeletedValuesOnFlush() {
        cache.setListener(new ThreadCache.DirtyEntryFlushListener() {
            
            public void apply(List<ThreadCache.DirtyEntry> dirty) {
                // no-op
            }
        });
        cache.put(Bytes.wrap(new byte[]{0}), new LRUCacheEntry(null, headers, true, 0, 0, 0, ""));
        cache.put(Bytes.wrap(new byte[]{1}), new LRUCacheEntry(new byte[]{20}, null, true, 0, 0, 0, ""));
        cache.flush();
        Assert.Equal(1, cache.Count);
        assertNotNull(cache.get(Bytes.wrap(new byte[]{1})));
    }

    [Xunit.Fact]
    public void ShouldBeReentrantAndNotBreakLRU() {
        LRUCacheEntry dirty = new LRUCacheEntry(new byte[]{3}, null, true, 0, 0, 0, "");
        LRUCacheEntry clean = new LRUCacheEntry(new byte[]{3});
        cache.put(Bytes.wrap(new byte[]{0}), dirty);
        cache.put(Bytes.wrap(new byte[]{1}), clean);
        cache.put(Bytes.wrap(new byte[]{2}), clean);
        Assert.Equal(3 * cache.head().Count, cache.sizeInBytes());
        cache.setListener(new ThreadCache.DirtyEntryFlushListener() {
            
            public void apply(List<ThreadCache.DirtyEntry> dirty) {
                cache.put(Bytes.wrap(new byte[]{3}), clean);
                // evict key 1
                cache.evict();
                // evict key 2
                cache.evict();
            }
        });

        Assert.Equal(3 * cache.Head().Count, cache.sizeInBytes());
        // Evict key 0
        cache.evict();
        Bytes entryFour = Bytes.wrap(new byte[]{4});
        cache.put(entryFour, dirty);

        // check that the LRU is still correct
        NamedCache.LRUNode head = cache.head();
        NamedCache.LRUNode tail = cache.tail();
        Assert.Equal(2, cache.Count);
        Assert.Equal(2 * head.Count, cache.sizeInBytes());
        // dirty should be the newest
        Assert.Equal(entryFour, head.Key);
        Assert.Equal(Bytes.wrap(new byte[] {3}), tail.Key);
        assertSame(tail, head.next());
        assertNull(head.previous());
        assertSame(head, tail.previous());
        assertNull(tail.next());

        // evict key 3
        cache.evict();
        assertSame(cache.head(), cache.tail());
        Assert.Equal(entryFour, cache.head().Key);
        assertNull(cache.head().next());
        assertNull(cache.head().previous());
    }

    [Xunit.Fact]
    public void ShouldNotThrowIllegalArgumentAfterEvictingDirtyRecordAndThenPuttingNewRecordWithSameKey() {
        LRUCacheEntry dirty = new LRUCacheEntry(new byte[]{3}, null, true, 0, 0, 0, "");
        LRUCacheEntry clean = new LRUCacheEntry(new byte[]{3});
        Bytes key = Bytes.wrap(new byte[] {3});
        cache.setListener(new ThreadCache.DirtyEntryFlushListener() {
            
            public void apply(List<ThreadCache.DirtyEntry> dirty) {
                cache.put(key, clean);
            }
        });
        cache.put(key, dirty);
        cache.evict();
    }

    [Xunit.Fact]
    public void ShouldReturnNullIfKeyIsNull() {
        assertNull(cache.get(null));
    }
}