/*






 *

 *





 */

















public class RocksDBSessionStoreTest : SessionBytesStoreTest {

    private static readonly string STORE_NAME = "rocksDB session store";

    
    SessionStore<K, V> BuildSessionStore<K, V>(long retentionPeriod,
                                                 Serde<K> keySerde,
                                                 Serde<V> valueSerde) {
        return Stores.sessionStoreBuilder(
            Stores.persistentSessionStore(
                STORE_NAME,
                ofMillis(retentionPeriod)),
            keySerde,
            valueSerde).build();
    }

    
    string GetMetricsScope() {
        return new RocksDbSessionBytesStoreSupplier(null, 0).metricsScope();
    }

    
    void SetClassLoggerToDebug() {
        LogCaptureAppender.setClassLoggerToDebug(AbstractRocksDBSegmentedBytesStore);
    }

    [Xunit.Fact]
    public void ShouldRemoveExpired() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, SEGMENT_INTERVAL)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, SEGMENT_INTERVAL)), 3L);

        // Advance stream time to expire the first record
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, 2 * SEGMENT_INTERVAL)), 4L);

        try (KeyValueIterator<Windowed<string>, long> iterator =
            sessionStore.findSessions("a", "b", 0L, long.MaxValue)
        ) {
            Assert.Equal(valuesToSet(iterator), new HashSet<>(Array.asList(2L, 3L, 4L)));
        }
    }
}