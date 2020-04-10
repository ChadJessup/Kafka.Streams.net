//namespace Kafka.Streams.Tests.State.Internals
//{
//    /*






//    *

//    *





//    */

















//    public class RocksDBSessionStoreTest : SessionBytesStoreTest
//    {

//        private const string STORE_NAME = "rocksDB session store";


//        ISessionStore<K, V> BuildSessionStore<K, V>(long retentionPeriod,
//                                                     Serde<K> keySerde,
//                                                     Serde<V> valueSerde)
//        {
//            return Stores.sessionStoreBuilder(
//                Stores.PersistentSessionStore(
//                    STORE_NAME,
//                    FromMilliseconds(retentionPeriod)),
//                keySerde,
//                valueSerde).Build();
//        }


//        string GetMetricsScope()
//        {
//            return new RocksDbSessionBytesStoreSupplier(null, 0).metricsScope();
//        }


//        void SetClassLoggerToDebug()
//        {
//            LogCaptureAppender.setClassLoggerToDebug(AbstractRocksDBSegmentedBytesStore);
//        }

//        [Fact]
//        public void ShouldRemoveExpired()
//        {
//            sessionStore.Put(new IWindowed<>("a", new SessionWindow(0, 0)), 1L);
//            sessionStore.Put(new IWindowed<>("aa", new SessionWindow(0, SEGMENT_INTERVAL)), 2L);
//            sessionStore.Put(new IWindowed<>("a", new SessionWindow(10, SEGMENT_INTERVAL)), 3L);

//            // Advance stream time to expire the first record
//            sessionStore.Put(new IWindowed<>("aa", new SessionWindow(10, 2 * SEGMENT_INTERVAL)), 4L);

//            try (IKeyValueIterator<IWindowed<string>, long> iterator =
//                sessionStore.findSessions("a", "b", 0L, long.MaxValue)
//            ) {
//                Assert.Equal(valuesToSet(iterator), new HashSet<>(Array.asList(2L, 3L, 4L)));
//            }
//            }
//}}
///*






//*

//*





//*/

















