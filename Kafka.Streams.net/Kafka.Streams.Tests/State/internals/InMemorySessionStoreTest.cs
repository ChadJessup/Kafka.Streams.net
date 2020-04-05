//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.Sessions;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class InMemorySessionStoreTest : SessionBytesStoreTest
//    {
//        private const string STORE_NAME = "in-memory session store";

//        ISessionStore<K, V> BuildSessionStore<K, V>(
//            long retentionPeriod,
//            ISerde<K> keySerde,
//            ISerde<V> valueSerde)
//        {
//            return Stores.sessionStoreBuilder(
//                Stores.inMemorySessionStore(
//                    STORE_NAME,
//                    FromMilliseconds(retentionPeriod)),
//                keySerde,
//                valueSerde).Build();
//        }

//        //string GetMetricsScope()
//        //{
//        //    return new InMemorySessionBytesStoreSupplier(null, 0).metricsScope();
//        //}

//        void SetClassLoggerToDebug()
//        {
//            LogCaptureAppender.setClassLoggerToDebug(InMemorySessionStore);
//        }

//        [Fact]
//        public void ShouldRemoveExpired()
//        {
//            sessionStore.put(new Windowed<string>("a", new SessionWindow(0, 0)), 1L);
//            sessionStore.put(new Windowed<string>("aa", new SessionWindow(0, 10)), 2L);
//            sessionStore.put(new Windowed<string>("a", new SessionWindow(10, 20)), 3L);

//            // Advance stream time to expire the first record
//            sessionStore.put(new Windowed<string>("aa", new SessionWindow(10, RETENTION_PERIOD)), 4L);

//            IKeyValueIterator<Windowed<string>, long> iterator =
//                sessionStore.findSessions("a", "b", 0L, long.MaxValue);
//            Assert.Equal(valuesToSet(iterator), new HashSet<>(Array.asList(2L, 3L, 4L)));
//        }

//        [Fact]
//        public void ShouldNotExpireFromOpenIterator()
//        {
//            sessionStore.put(new Windowed<string>("a", new SessionWindow(0, 0)), 1L);
//            sessionStore.put(new Windowed<string>("aa", new SessionWindow(0, 10)), 2L);
//            sessionStore.put(new Windowed<string>("a", new SessionWindow(10, 20)), 3L);

//            IKeyValueIterator<Windowed<string>, long> iterator = sessionStore.findSessions("a", "b", 0L, RETENTION_PERIOD);

//            // Advance stream time to expire the first three record
//            sessionStore.put(new Windowed<string>("aa", new SessionWindow(100, 2 * RETENTION_PERIOD)), 4L);

//            Assert.Equal(valuesToSet(iterator), new HashSet<>(Array.asList(1L, 2L, 3L, 4L)));
//            Assert.False(iterator.hasNext());

//            iterator.close();
//            Assert.False(sessionStore.findSessions("a", "b", 0L, 20L).hasNext());
//        }
//    }
//}
