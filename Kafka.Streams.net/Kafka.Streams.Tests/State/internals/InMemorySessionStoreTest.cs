/*






 *

 *





 */




















public class InMemorySessionStoreTest : SessionBytesStoreTest {

    private static string STORE_NAME = "in-memory session store";

    
    SessionStore<K, V> BuildSessionStore<K, V>(long retentionPeriod,
                                                 Serde<K> keySerde,
                                                 Serde<V> valueSerde) {
        return Stores.sessionStoreBuilder(
            Stores.inMemorySessionStore(
                STORE_NAME,
                ofMillis(retentionPeriod)),
            keySerde,
            valueSerde).build();
    }

    
    string GetMetricsScope() {
        return new InMemorySessionBytesStoreSupplier(null, 0).metricsScope();
    }

    
    void SetClassLoggerToDebug() {
        LogCaptureAppender.setClassLoggerToDebug(InMemorySessionStore);
    }

    [Xunit.Fact]
    public void ShouldRemoveExpired() {
        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);

        // Advance stream time to expire the first record
        sessionStore.put(new Windowed<>("aa", new SessionWindow(10, RETENTION_PERIOD)), 4L);

        try (KeyValueIterator<Windowed<string>, long> iterator =
            sessionStore.findSessions("a", "b", 0L, long.MaxValue)
        ) {
            Assert.Equal(valuesToSet(iterator), new HashSet<>(Array.asList(2L, 3L, 4L)));
        }
    }

    [Xunit.Fact]
    public void ShouldNotExpireFromOpenIterator() {

        sessionStore.put(new Windowed<>("a", new SessionWindow(0, 0)), 1L);
        sessionStore.put(new Windowed<>("aa", new SessionWindow(0, 10)), 2L);
        sessionStore.put(new Windowed<>("a", new SessionWindow(10, 20)), 3L);

        KeyValueIterator<Windowed<string>, long> iterator = sessionStore.findSessions("a", "b", 0L, RETENTION_PERIOD);

        // Advance stream time to expire the first three record
        sessionStore.put(new Windowed<>("aa", new SessionWindow(100, 2 * RETENTION_PERIOD)), 4L);

        Assert.Equal(valuesToSet(iterator), new HashSet<>(Array.asList(1L, 2L, 3L, 4L)));
        Assert.False(iterator.hasNext());

        iterator.close();
        Assert.False(sessionStore.findSessions("a", "b", 0L, 20L).hasNext());
    }

}