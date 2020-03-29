/*






 *

 *





 */
















































public class CachingSessionStoreTest {

    private static int MAX_CACHE_SIZE_BYTES = 600;
    private static long DEFAULT_TIMESTAMP = 10L;
    private static long SEGMENT_INTERVAL = 100L;
    private Bytes keyA = Bytes.wrap("a".getBytes());
    private Bytes keyAA = Bytes.wrap("aa".getBytes());
    private Bytes keyB = Bytes.wrap("b".getBytes());

    private CachingSessionStore cachingStore;
    private ThreadCache cache;

    public CachingSessionStoreTest() {
        SessionKeySchema schema = new SessionKeySchema();
        RocksDBSegmentedBytesStore root =
            new RocksDBSegmentedBytesStore("test", "metrics-scope", 0L, SEGMENT_INTERVAL, schema);
        RocksDBSessionStore sessionStore = new RocksDBSessionStore(root);
        cachingStore = new CachingSessionStore(sessionStore, SEGMENT_INTERVAL);
        cache = new ThreadCache(new LogContext("testCache "), MAX_CACHE_SIZE_BYTES, new MockStreamsMetrics(new Metrics()));
        InternalMockProcessorContext context = new InternalMockProcessorContext(TestUtils.tempDirectory(), null, null, null, cache);
        context.setRecordContext(new ProcessorRecordContext(DEFAULT_TIMESTAMP, 0, 0, "topic", null));
        cachingStore.init(context, cachingStore);
    }

    
    public void Close() {
        cachingStore.close();
    }

    [Xunit.Fact]
    public void ShouldPutFetchFromCache() {
        cachingStore.put(new Windowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes());
        cachingStore.put(new Windowed<>(keyAA, new SessionWindow(0, 0)), "1".getBytes());
        cachingStore.put(new Windowed<>(keyB, new SessionWindow(0, 0)), "1".getBytes());

        Assert.Equal(3, cache.Count);

        KeyValueIterator<Windowed<Bytes>, byte[]> a = cachingStore.findSessions(keyA, 0, 0);
        KeyValueIterator<Windowed<Bytes>, byte[]> b = cachingStore.findSessions(keyB, 0, 0);

        verifyWindowedKeyValue(a.next(), new Windowed<>(keyA, new SessionWindow(0, 0)), "1");
        verifyWindowedKeyValue(b.next(), new Windowed<>(keyB, new SessionWindow(0, 0)), "1");
        Assert.False(a.hasNext());
        Assert.False(b.hasNext());
    }

    [Xunit.Fact]
    public void ShouldPutFetchAllKeysFromCache() {
        cachingStore.put(new Windowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes());
        cachingStore.put(new Windowed<>(keyAA, new SessionWindow(0, 0)), "1".getBytes());
        cachingStore.put(new Windowed<>(keyB, new SessionWindow(0, 0)), "1".getBytes());

        Assert.Equal(3, cache.Count);

        KeyValueIterator<Windowed<Bytes>, byte[]> all = cachingStore.findSessions(keyA, keyB, 0, 0);
        verifyWindowedKeyValue(all.next(), new Windowed<>(keyA, new SessionWindow(0, 0)), "1");
        verifyWindowedKeyValue(all.next(), new Windowed<>(keyAA, new SessionWindow(0, 0)), "1");
        verifyWindowedKeyValue(all.next(), new Windowed<>(keyB, new SessionWindow(0, 0)), "1");
        Assert.False(all.hasNext());
    }

    [Xunit.Fact]
    public void ShouldPutFetchRangeFromCache() {
        cachingStore.put(new Windowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes());
        cachingStore.put(new Windowed<>(keyAA, new SessionWindow(0, 0)), "1".getBytes());
        cachingStore.put(new Windowed<>(keyB, new SessionWindow(0, 0)), "1".getBytes());

        Assert.Equal(3, cache.Count);

        KeyValueIterator<Windowed<Bytes>, byte[]> some = cachingStore.findSessions(keyAA, keyB, 0, 0);
        verifyWindowedKeyValue(some.next(), new Windowed<>(keyAA, new SessionWindow(0, 0)), "1");
        verifyWindowedKeyValue(some.next(), new Windowed<>(keyB, new SessionWindow(0, 0)), "1");
        Assert.False(some.hasNext());
    }

    [Xunit.Fact]
    public void ShouldFetchAllSessionsWithSameRecordKey() {
        List<KeyValuePair<Windowed<Bytes>, byte[]>> expected = asList(
            KeyValuePair.Create(new Windowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes()),
            KeyValuePair.Create(new Windowed<>(keyA, new SessionWindow(10, 10)), "2".getBytes()),
            KeyValuePair.Create(new Windowed<>(keyA, new SessionWindow(100, 100)), "3".getBytes()),
            KeyValuePair.Create(new Windowed<>(keyA, new SessionWindow(1000, 1000)), "4".getBytes())
        );
        foreach (KeyValuePair<Windowed<Bytes>, byte[]> kv in expected) {
            cachingStore.put(kv.key, kv.value);
        }

        // add one that shouldn't appear in the results
        cachingStore.put(new Windowed<>(keyAA, new SessionWindow(0, 0)), "5".getBytes());

        List<KeyValuePair<Windowed<Bytes>, byte[]>> results = toList(cachingStore.fetch(keyA));
        verifyKeyValueList(expected, results);
    }

    [Xunit.Fact]
    public void ShouldFlushItemsToStoreOnEviction() {
        List<KeyValuePair<Windowed<Bytes>, byte[]>> added = addSessionsUntilOverflow("a", "b", "c", "d");
        Assert.Equal(added.Count - 1, cache.Count);
        KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.findSessions(added.get(0).key.Key, 0, 0);
        KeyValuePair<Windowed<Bytes>, byte[]> next = iterator.next();
        Assert.Equal(added.get(0).key, next.key);
        assertArrayEquals(added.get(0).value, next.value);
    }

    [Xunit.Fact]
    public void ShouldQueryItemsInCacheAndStore() {
        List<KeyValuePair<Windowed<Bytes>, byte[]>> added = addSessionsUntilOverflow("a");
        KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.findSessions(
            Bytes.wrap("a".getBytes(StandardCharsets.UTF_8)),
            0,
            added.Count * 10);
        List<KeyValuePair<Windowed<Bytes>, byte[]>> actual = toList(iterator);
        verifyKeyValueList(added, actual);
    }

    [Xunit.Fact]
    public void ShouldRemove() {
        Windowed<Bytes> a = new Windowed<>(keyA, new SessionWindow(0, 0));
        Windowed<Bytes> b = new Windowed<>(keyB, new SessionWindow(0, 0));
        cachingStore.put(a, "2".getBytes());
        cachingStore.put(b, "2".getBytes());
        cachingStore.remove(a);

        KeyValueIterator<Windowed<Bytes>, byte[]> rangeIter =
            cachingStore.findSessions(keyA, 0, 0);
        Assert.False(rangeIter.hasNext());

        assertNull(cachingStore.fetchSession(keyA, 0, 0));
        Assert.Equal(cachingStore.fetchSession(keyB, 0, 0), ("2".getBytes()));

    }

    [Xunit.Fact]
    public void ShouldFetchCorrectlyAcrossSegments() {
        Windowed<Bytes> a1 = new Windowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 0, SEGMENT_INTERVAL * 0));
        Windowed<Bytes> a2 = new Windowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 1, SEGMENT_INTERVAL * 1));
        Windowed<Bytes> a3 = new Windowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 2, SEGMENT_INTERVAL * 2));
        cachingStore.put(a1, "1".getBytes());
        cachingStore.put(a2, "2".getBytes());
        cachingStore.put(a3, "3".getBytes());
        cachingStore.flush();
        KeyValueIterator<Windowed<Bytes>, byte[]> results =
            cachingStore.findSessions(keyA, 0, SEGMENT_INTERVAL * 2);
        Assert.Equal(a1, results.next().key);
        Assert.Equal(a2, results.next().key);
        Assert.Equal(a3, results.next().key);
        Assert.False(results.hasNext());
    }

    [Xunit.Fact]
    public void ShouldFetchRangeCorrectlyAcrossSegments() {
        Windowed<Bytes> a1 = new Windowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 0, SEGMENT_INTERVAL * 0));
        Windowed<Bytes> aa1 = new Windowed<>(keyAA, new SessionWindow(SEGMENT_INTERVAL * 0, SEGMENT_INTERVAL * 0));
        Windowed<Bytes> a2 = new Windowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 1, SEGMENT_INTERVAL * 1));
        Windowed<Bytes> a3 = new Windowed<>(keyA, new SessionWindow(SEGMENT_INTERVAL * 2, SEGMENT_INTERVAL * 2));
        Windowed<Bytes> aa3 = new Windowed<>(keyAA, new SessionWindow(SEGMENT_INTERVAL * 2, SEGMENT_INTERVAL * 2));
        cachingStore.put(a1, "1".getBytes());
        cachingStore.put(aa1, "1".getBytes());
        cachingStore.put(a2, "2".getBytes());
        cachingStore.put(a3, "3".getBytes());
        cachingStore.put(aa3, "3".getBytes());

        KeyValueIterator<Windowed<Bytes>, byte[]> rangeResults =
            cachingStore.findSessions(keyA, keyAA, 0, SEGMENT_INTERVAL * 2);
        HashSet<Windowed<Bytes>> keys = new HashSet<>();
        while (rangeResults.hasNext()) {
            keys.add(rangeResults.next().key);
        }
        rangeResults.close();
        Assert.Equal(mkSet(a1, a2, a3, aa1, aa3), keys);
    }

    [Xunit.Fact]
    public void ShouldSetFlushListener() {
        Assert.True(cachingStore.setFlushListener(null, true));
        Assert.True(cachingStore.setFlushListener(null, false));
    }

    [Xunit.Fact]
    public void ShouldForwardChangedValuesDuringFlush() {
        Windowed<Bytes> a = new Windowed<>(keyA, new SessionWindow(2, 4));
        Windowed<Bytes> b = new Windowed<>(keyA, new SessionWindow(1, 2));
        Windowed<string> aDeserialized = new Windowed<>("a", new SessionWindow(2, 4));
        Windowed<string> bDeserialized = new Windowed<>("a", new SessionWindow(1, 2));
        CacheFlushListenerStub<Windowed<string>, string> flushListener =
            new CacheFlushListenerStub<>(
                new SessionWindowedDeserializer<>(new StringDeserializer()),
                new StringDeserializer());
        cachingStore.setFlushListener(flushListener, true);

        cachingStore.put(b, "1".getBytes());
        cachingStore.flush();

        Assert.Equal(
            Collections.singletonList(
                new KeyValueTimestamp<>(
                    bDeserialized,
                    new Change<>("1", null),
                    DEFAULT_TIMESTAMP)),
            flushListener.forwarded
        );
        flushListener.forwarded.Clear();

        cachingStore.put(a, "1".getBytes());
        cachingStore.flush();

        Assert.Equal(
            Collections.singletonList(
                new KeyValueTimestamp<>(
                    aDeserialized,
                    new Change<>("1", null),
                    DEFAULT_TIMESTAMP)),
            flushListener.forwarded
        );
        flushListener.forwarded.Clear();

        cachingStore.put(a, "2".getBytes());
        cachingStore.flush();

        Assert.Equal(
            Collections.singletonList(
                new KeyValueTimestamp<>(
                    aDeserialized,
                    new Change<>("2", "1"),
                    DEFAULT_TIMESTAMP)),
            flushListener.forwarded
        );
        flushListener.forwarded.Clear();

        cachingStore.remove(a);
        cachingStore.flush();

        Assert.Equal(
            Collections.singletonList(
                new KeyValueTimestamp<>(
                    aDeserialized,
                    new Change<>(null, "2"),
                    DEFAULT_TIMESTAMP)),
            flushListener.forwarded
        );
        flushListener.forwarded.Clear();

        cachingStore.put(a, "1".getBytes());
        cachingStore.put(a, "2".getBytes());
        cachingStore.remove(a);
        cachingStore.flush();

        Assert.Equal(
            Collections.emptyList(),
            flushListener.forwarded
        );
        flushListener.forwarded.Clear();
    }

    [Xunit.Fact]
    public void ShouldNotForwardChangedValuesDuringFlushWhenSendOldValuesDisabled() {
        Windowed<Bytes> a = new Windowed<>(keyA, new SessionWindow(0, 0));
        Windowed<string> aDeserialized = new Windowed<>("a", new SessionWindow(0, 0));
        CacheFlushListenerStub<Windowed<string>, string> flushListener =
            new CacheFlushListenerStub<>(
                new SessionWindowedDeserializer<>(new StringDeserializer()),
                new StringDeserializer());
        cachingStore.setFlushListener(flushListener, false);

        cachingStore.put(a, "1".getBytes());
        cachingStore.flush();

        cachingStore.put(a, "2".getBytes());
        cachingStore.flush();

        cachingStore.remove(a);
        cachingStore.flush();

        Assert.Equal(
            asList(new KeyValueTimestamp<>(
                    aDeserialized,
                    new Change<>("1", null),
                    DEFAULT_TIMESTAMP),
                new KeyValueTimestamp<>(
                    aDeserialized,
                    new Change<>("2", null),
                    DEFAULT_TIMESTAMP),
                new KeyValueTimestamp<>(
                    aDeserialized,
                    new Change<>(null, null),
                    DEFAULT_TIMESTAMP)),
            flushListener.forwarded
        );
        flushListener.forwarded.Clear();

        cachingStore.put(a, "1".getBytes());
        cachingStore.put(a, "2".getBytes());
        cachingStore.remove(a);
        cachingStore.flush();

        Assert.Equal(
            Collections.emptyList(),
            flushListener.forwarded
        );
        flushListener.forwarded.Clear();
    }

    [Xunit.Fact]
    public void ShouldReturnSameResultsForSingleKeyFindSessionsAndEqualKeyRangeFindSessions() {
        cachingStore.put(new Windowed<>(keyA, new SessionWindow(0, 1)), "1".getBytes());
        cachingStore.put(new Windowed<>(keyAA, new SessionWindow(2, 3)), "2".getBytes());
        cachingStore.put(new Windowed<>(keyAA, new SessionWindow(4, 5)), "3".getBytes());
        cachingStore.put(new Windowed<>(keyB, new SessionWindow(6, 7)), "4".getBytes());

        KeyValueIterator<Windowed<Bytes>, byte[]> singleKeyIterator = cachingStore.findSessions(keyAA, 0L, 10L);
        KeyValueIterator<Windowed<Bytes>, byte[]> keyRangeIterator = cachingStore.findSessions(keyAA, keyAA, 0L, 10L);

        Assert.Equal(singleKeyIterator.next(), keyRangeIterator.next());
        Assert.Equal(singleKeyIterator.next(), keyRangeIterator.next());
        Assert.False(singleKeyIterator.hasNext());
        Assert.False(keyRangeIterator.hasNext());
    }

    [Xunit.Fact]
    public void ShouldClearNamespaceCacheOnClose() {
        Windowed<Bytes> a1 = new Windowed<>(keyA, new SessionWindow(0, 0));
        cachingStore.put(a1, "1".getBytes());
        Assert.Equal(1, cache.Count);
        cachingStore.close();
        Assert.Equal(0, cache.Count);
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowIfTryingToFetchFromClosedCachingStore() {
        cachingStore.close();
        cachingStore.fetch(keyA);
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowIfTryingToFindMergeSessionFromClosedCachingStore() {
        cachingStore.close();
        cachingStore.findSessions(keyA, 0, long.MaxValue);
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowIfTryingToRemoveFromClosedCachingStore() {
        cachingStore.close();
        cachingStore.remove(new Windowed<>(keyA, new SessionWindow(0, 0)));
    }

    [Xunit.Fact]// (expected = InvalidStateStoreException)
    public void ShouldThrowIfTryingToPutIntoClosedCachingStore() {
        cachingStore.close();
        cachingStore.put(new Windowed<>(keyA, new SessionWindow(0, 0)), "1".getBytes());
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnFindSessionsNullKey() {
        cachingStore.findSessions(null, 1L, 2L);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnFindSessionsNullFromKey() {
        cachingStore.findSessions(null, keyA, 1L, 2L);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnFindSessionsNullToKey() {
        cachingStore.findSessions(keyA, null, 1L, 2L);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnFetchNullFromKey() {
        cachingStore.fetch(null, keyA);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnFetchNullToKey() {
        cachingStore.fetch(keyA, null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnFetchNullKey() {
        cachingStore.fetch(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnRemoveNullKey() {
        cachingStore.remove(null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void ShouldThrowNullPointerExceptionOnPutNullKey() {
        cachingStore.put(null, "1".getBytes());
    }

    [Xunit.Fact]
    public void ShouldNotThrowInvalidRangeExceptionWithNegativeFromKey() {
        LogCaptureAppender.setClassLoggerToDebug(InMemoryWindowStore);
        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

        Bytes keyFrom = Bytes.wrap(Serdes.Int().Serializer.serialize("", -1));
        Bytes keyTo = Bytes.wrap(Serdes.Int().Serializer.serialize("", 1));

        KeyValueIterator<Windowed<Bytes>, byte[]> iterator = cachingStore.findSessions(keyFrom, keyTo, 0L, 10L);
        Assert.False(iterator.hasNext());

        List<string> messages = appender.getMessages();
        Assert.Equal(messages, hasItem("Returning empty iterator for fetch with invalid key range: from > to. "
            + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
            + "Note that the built-in numerical serdes do not follow this for negative numbers"));
    }

    private List<KeyValuePair<Windowed<Bytes>, byte[]>> AddSessionsUntilOverflow(string... sessionIds) {
        Random random = new Random();
        List<KeyValuePair<Windowed<Bytes>, byte[]>> results = new ArrayList<>();
        while (cache.Count == results.Count) {
            string sessionId = sessionIds[random.nextInt(sessionIds.Length)];
            AddSingleSession(sessionId, results);
        }
        return results;
    }

    private void AddSingleSession(string sessionId, List<KeyValuePair<Windowed<Bytes>, byte[]>> allSessions) {
        int timestamp = allSessions.Count * 10;
        Windowed<Bytes> key = new Windowed<>(Bytes.wrap(sessionId.getBytes()), new SessionWindow(timestamp, timestamp));
        byte[] value = "1".getBytes();
        cachingStore.put(key, value);
        allSessions.add(KeyValuePair.Create(key, value));
    }

    public static class CacheFlushListenerStub<K, V> : CacheFlushListener<byte[], byte[]> {
        Deserializer<K> keyDeserializer;
        Deserializer<V> valueDesializer;
        List<KeyValueTimestamp<K, Change<V>>> forwarded = new LinkedList<>();

        CacheFlushListenerStub(Deserializer<K> keyDeserializer,
                               Deserializer<V> valueDesializer) {
            this.keyDeserializer = keyDeserializer;
            this.valueDesializer = valueDesializer;
        }

        
        public void Apply(byte[] key,
                          byte[] newValue,
                          byte[] oldValue,
                          long timestamp) {
            forwarded.add(
                new KeyValueTimestamp<>(
                    keyDeserializer.deserialize(null, key),
                    new Change<>(
                        valueDesializer.deserialize(null, newValue),
                        valueDesializer.deserialize(null, oldValue)),
                    timestamp));
        }
    }
}
