//using Confluent.Kafka;
//using Kafka.Streams.Errors;
//using Kafka.Streams.Interfaces;
//using Kafka.Streams.KStream;
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.Sessions;
//using System;
//using System.Collections.Generic;
//using Xunit;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public abstract class SessionBytesStoreTest
//    {
//        protected static long SEGMENT_INTERVAL = 60_000L;
//        protected static long RETENTION_PERIOD = 10_000L;

//        protected ISessionStore<string, long> sessionStore;
//        protected InternalMockProcessorContext context;

//        private List<KeyValuePair<byte[], byte[]>> changeLog = new List<KeyValuePair<byte[], byte[]>>();

//        private IProducer<byte[], byte[]> producer = new MockProducer<>(true,
//            Serdes.ByteArray().Serializer,
//            Serdes.ByteArray().Serializer);

//        public abstract ISessionStore<K, V> BuildSessionStore<K, V>(
//            long retentionPeriod,
//            ISerde<K> keySerde,
//            ISerde<V> valueSerde);

//        public abstract string GetMetricsScope();
//        public abstract void SetClassLoggerToDebug();

//        private RecordCollector CreateRecordCollector(string Name)
//        {
//            return new RecordCollector(Name,
//                new LogContext(Name),
//                new DefaultProductionExceptionHandler());
//            //{


//            //public void send<K1, V1>(string topic,
//            //    K1 key,
//            //    V1 value,
//            //    Headers headers,
//            //    int partition,
//            //    long timestamp,
//            //    ISerializer<K1> keySerializer,
//            //    ISerializer<V1> valueSerializer)
//            //{
//            //    changeLog.Add(KeyValuePair.Create(
//            //        keySerializer.Serialize(topic, headers, key),
//            //        valueSerializer.Serialize(topic, headers, value)));
//            //}
//        }

//        public void SetUp()
//        {
//            sessionStore = buildSessionStore(RETENTION_PERIOD, Serdes.String(), Serdes.Long());

//            RecordCollector recordCollector = createRecordCollector(sessionStore.Name());
//            recordCollector.Init(producer);

//            context = new InternalMockProcessorContext(
//                TestUtils.GetTempDirectory(),
//                Serdes.String(),
//                Serdes.Long(),
//                recordCollector,
//                new ThreadCache(
//                    new LogContext("testCache"),
//                    0,
//                    new MockStreamsMetrics(new Metrics())));

//            sessionStore.Init(context, sessionStore);
//        }

//        public void After()
//        {
//            sessionStore.Close();
//        }

//        [Fact]
//        public void ShouldPutAndFindSessionsInRange()
//        {
//            string key = "a";
//            IWindowed<string> a1 = new Windowed<string>(key, new SessionWindow(10, 10L));
//            IWindowed<string> a2 = new Windowed<string>(key, new SessionWindow(500L, 1000L));
//            sessionStore.Put(a1, 1L);
//            sessionStore.Put(a2, 2L);
//            sessionStore.Put(new Windowed<string>(key, new SessionWindow(1500L, 2000L)), 1L);
//            sessionStore.Put(new Windowed<string>(key, new SessionWindow(2500L, 3000L)), 2L);

//            List<KeyValuePair<IWindowed<string>, long>> expected =
//                Arrays.asList(KeyValuePair.Create(a1, 1L), KeyValuePair.Create(a2, 2L));

//            IKeyValueIterator<IWindowed<string>, long> values = sessionStore.findSessions(key, 0, 1000L);
//            Assert.Equal(new HashSet<>(expected), toSet(values));

//            List<KeyValuePair<IWindowed<string>, long>> expected2 =
//                Collections.singletonList(KeyValuePair.Create(a2, 2L));

//            IKeyValueIterator<IWindowed<string>, long> values2 = sessionStore.findSessions(key, 400L, 600L);
//            Assert.Equal(new HashSet<>(expected2), toSet(values2));
//        }

//        [Fact]
//        public void ShouldFetchAllSessionsWithSameRecordKey()
//        {
//            List<KeyValuePair<IWindowed<string>, long>> expected = Arrays.asList(
//                KeyValuePair.Create(new Windowed<string>("a", new SessionWindow(0, 0)), 1L),
//                KeyValuePair.Create(new Windowed<string>("a", new SessionWindow(10, 10)), 2L),
//                KeyValuePair.Create(new Windowed<string>("a", new SessionWindow(100, 100)), 3L),
//                KeyValuePair.Create(new Windowed<string>("a", new SessionWindow(1000, 1000)), 4L));

//            foreach (KeyValuePair<IWindowed<string>, long> kv in expected)
//            {
//                sessionStore.Put(kv.Key, kv.Value);
//            }

//            // add one that shouldn't appear in the results
//            sessionStore.Put(new Windowed<string>("aa", new SessionWindow(0, 0)), 5L);

//            IKeyValueIterator<IWindowed<string>, long> values = sessionStore.Fetch("a");
//            Assert.Equal(new HashSet<>(expected), toSet(values));
//        }

//        [Fact]
//        public void ShouldFetchAllSessionsWithinKeyRange()
//        {
//            List<KeyValuePair<IWindowed<string>, long>> expected = Arrays.asList(
//                KeyValuePair.Create(new Windowed<string>("aa", new SessionWindow(10, 10)), 2L),
//                KeyValuePair.Create(new Windowed<string>("b", new SessionWindow(1000, 1000)), 4L),

//            KeyValuePair.Create(new Windowed<string>("aaa", new SessionWindow(100, 100)), 3L),
//            KeyValuePair.Create(new Windowed<string>("bb", new SessionWindow(1500, 2000)), 5L));

//            foreach (KeyValuePair<IWindowed<string>, long> kv in expected)
//            {
//                sessionStore.Put(kv.Key, kv.Value);
//            }

//            // add some that shouldn't appear in the results
//            sessionStore.Put(new Windowed<string>("a", new SessionWindow(0, 0)), 1L);
//            sessionStore.Put(new Windowed<string>("bbb", new SessionWindow(2500, 3000)), 6L);

//            IKeyValueIterator<IWindowed<string>, long> values = sessionStore.Fetch("aa", "bb");
//            Assert.Equal(new HashSet<>(expected), toSet(values));
//        }

//        [Fact]
//        public void ShouldFetchExactSession()
//        {
//            sessionStore.Put(new Windowed<string>("a", new SessionWindow(0, 4)), 1L);
//            sessionStore.Put(new Windowed<string>("aa", new SessionWindow(0, 3)), 2L);
//            sessionStore.Put(new Windowed<string>("aa", new SessionWindow(0, 4)), 3L);
//            sessionStore.Put(new Windowed<string>("aa", new SessionWindow(1, 4)), 4L);
//            sessionStore.Put(new Windowed<string>("aaa", new SessionWindow(0, 4)), 5L);

//            long result = sessionStore.FetchSession("aa", 0, 4);
//            Assert.Equal(3L, result);
//        }

//        [Fact]
//        public void ShouldReturnNullOnSessionNotFound()
//        {
//            Assert.Null(sessionStore.FetchSession("any key", 0L, 5L));
//        }

//        [Fact]
//        public void ShouldFindValuesWithinMergingSessionWindowRange()
//        {
//            string key = "a";
//            sessionStore.Put(new Windowed<string>(key, new SessionWindow(0L, 0L)), 1L);
//            sessionStore.Put(new Windowed<string>(key, new SessionWindow(1000L, 1000L)), 2L);

//            List<KeyValuePair<IWindowed<string>, long>> expected = Arrays.asList(
//                KeyValuePair.Create(new Windowed<string>(key, new SessionWindow(0L, 0L)), 1L),
//                KeyValuePair.Create(new Windowed<string>(key, new SessionWindow(1000L, 1000L)), 2L));

//            IKeyValueIterator<IWindowed<string>, long> results = sessionStore.findSessions(key, -1, 1000L);
//            Assert.Equal(new HashSet<>(expected), toSet(results));
//        }

//        [Fact]
//        public void ShouldRemove()
//        {
//            sessionStore.Put(new Windowed<string>("a", new SessionWindow(0, 1000)), 1L);
//            sessionStore.Put(new Windowed<string>("a", new SessionWindow(1500, 2500)), 2L);

//            sessionStore.remove(new Windowed<string>("a", new SessionWindow(0, 1000)));

//            IKeyValueIterator<IWindowed<string>, long> results = sessionStore.findSessions("a", 0L, 1000L);
//            Assert.False(results.MoveNext());

//            IKeyValueIterator<IWindowed<string>, long> results = sessionStore.findSessions("a", 1500L, 2500L);
//            Assert.True(results.MoveNext());
//        }
//    }

//    [Fact]
//    public void ShouldRemoveOnNullAggValue()
//    {
//        sessionStore.Put(new Windowed<string>("a", new SessionWindow(0, 1000)), 1L);
//        sessionStore.Put(new Windowed<string>("a", new SessionWindow(1500, 2500)), 2L);
//        sessionStore.Put(new Windowed<string>("a", new SessionWindow(0, 1000)), null);

//        IKeyValueIterator<IWindowed<string>, long> results = sessionStore.findSessions("a", 0L, 1000L);
//        Assert.False(results.MoveNext());

//        IKeyValueIterator<IWindowed<string>, long> results = sessionStore.findSessions("a", 1500L, 2500L);
//        Assert.True(results.MoveNext());
//    }

//    [Fact]
//    public void ShouldFindSessionsToMerge()
//    {
//        var session1 = new Windowed<string>("a", new SessionWindow(0, 100));
//        var session2 = new Windowed<string>("a", new SessionWindow(101, 200));
//        var session3 = new Windowed<string>("a", new SessionWindow(201, 300));
//        var session4 = new Windowed<string>("a", new SessionWindow(301, 400));
//        var session5 = new Windowed<string>("a", new SessionWindow(401, 500));
//        sessionStore.Put(session1, 1L);
//        sessionStore.Put(session2, 2L);
//        sessionStore.Put(session3, 3L);
//        sessionStore.Put(session4, 4L);
//        sessionStore.Put(session5, 5L);

//        List<KeyValuePair<IWindowed<string>, long>> expected =
//            Arrays.asList(KeyValuePair.Create(session2, 2L), KeyValuePair.Create(session3, 3L));

//        IKeyValueIterator<IWindowed<string>, long> results = sessionStore.findSessions("a", 150, 300);
//        Assert.Equal(new HashSet<>(expected), toSet(results));
//    }

//    [Fact]
//    public void ShouldFetchExactKeys()
//    {
//        sessionStore = buildSessionStore(0x7a00000000000000L, Serdes.String(), Serdes.Long());
//        sessionStore.Init(context, sessionStore);

//        sessionStore.Put(new Windowed<string>("a", new SessionWindow(0, 0)), 1L);
//        sessionStore.Put(new Windowed<string>("aa", new SessionWindow(0, 10)), 2L);
//        sessionStore.Put(new Windowed<string>("a", new SessionWindow(10, 20)), 3L);
//        sessionStore.Put(new Windowed<string>("aa", new SessionWindow(10, 20)), 4L);
//        sessionStore.Put(new Windowed<string>("a",
//            new SessionWindow(0x7a00000000000000L - 2, 0x7a00000000000000L - 1)), 5L);

//        IKeyValueIterator<IWindowed<string>, long> iterator =
//            sessionStore.findSessions("a", 0, long.MaxValue);

//        Assert.Equal(valuesToSet(iterator), (new HashSet<>(asList(1L, 3L, 5L))));

//        IKeyValueIterator<IWindowed<string>, long> iterator =
//            sessionStore.findSessions("aa", 0, long.MaxValue);
//        Assert.Equal(valuesToSet(iterator), (new HashSet<>(asList(2L, 4L))));

//        IKeyValueIterator<IWindowed<string>, long> iterator =
//            sessionStore.findSessions("a", "aa", 0, long.MaxValue);

//        Assert.Equal(valuesToSet(iterator), (new HashSet<>(asList(1L, 2L, 3L, 4L, 5L))));

//        IKeyValueIterator<IWindowed<string>, long> iterator = sessionStore.findSessions("a", "aa", 10, 0);
//        Assert.Equal(valuesToSet(iterator), (new HashSet<>(Collections.singletonList(2L))));
//    }

//    [Fact]
//    public void ShouldFetchAndIterateOverExactBinaryKeys()
//    {
//        ISessionStore<Bytes, string> sessionStore =
//            buildSessionStore(RETENTION_PERIOD, Serdes.Bytes(), Serdes.String());

//        sessionStore.Init(context, sessionStore);

//        Bytes key1 = Bytes.Wrap(new byte[] { 0 });
//        Bytes key2 = Bytes.Wrap(new byte[] { 0, 0 });
//        Bytes key3 = Bytes.Wrap(new byte[] { 0, 0, 0 });

//        sessionStore.Put(new Windowed<Bytes>(key1, new SessionWindow(1, 100)), "1");
//        sessionStore.Put(new Windowed<Bytes>(key2, new SessionWindow(2, 100)), "2");
//        sessionStore.Put(new Windowed<Bytes>(key3, new SessionWindow(3, 100)), "3");
//        sessionStore.Put(new Windowed<Bytes>(key1, new SessionWindow(4, 100)), "4");
//        sessionStore.Put(new Windowed<Bytes>(key2, new SessionWindow(5, 100)), "5");
//        sessionStore.Put(new Windowed<Bytes>(key3, new SessionWindow(6, 100)), "6");
//        sessionStore.Put(new Windowed<Bytes>(key1, new SessionWindow(7, 100)), "7");
//        sessionStore.Put(new Windowed<Bytes>(key2, new SessionWindow(8, 100)), "8");
//        sessionStore.Put(new Windowed<Bytes>(key3, new SessionWindow(9, 100)), "9");

//        HashSet<string> expectedKey1 = new HashSet<string>(asList("1", "4", "7"));
//        Assert.Equal(valuesToSet(sessionStore.findSessions(key1, 0L, long.MaxValue)), (expectedKey1));
//        HashSet<string> expectedKey2 = new HashSet<string>(asList("2", "5", "8"));
//        Assert.Equal(valuesToSet(sessionStore.findSessions(key2, 0L, long.MaxValue)), (expectedKey2));
//        HashSet<string> expectedKey3 = new HashSet<string>(asList("3", "6", "9"));
//        Assert.Equal(valuesToSet(sessionStore.findSessions(key3, 0L, long.MaxValue)), (expectedKey3));
//    }

//    [Fact]
//    public void TestIteratorPeek()
//    {
//        sessionStore.Put(new Windowed<string>("a", new SessionWindow(0, 0)), 1L);
//        sessionStore.Put(new Windowed<string>("aa", new SessionWindow(0, 10)), 2L);
//        sessionStore.Put(new Windowed<string>("a", new SessionWindow(10, 20)), 3L);
//        sessionStore.Put(new Windowed<string>("aa", new SessionWindow(10, 20)), 4L);

//        IKeyValueIterator<IWindowed<string>, long> iterator = sessionStore.findSessions("a", 0L, 20);

//        Assert.Equal(iterator.PeekNextKey(), new Windowed<string>("a", new SessionWindow(0L, 0L)));
//        Assert.Equal(iterator.PeekNextKey(), iterator.MoveNext().Key);
//        Assert.Equal(iterator.PeekNextKey(), iterator.MoveNext().Key);
//        Assert.False(iterator.MoveNext());
//    }

//    [Fact]
//    public void ShouldRestore()
//    {
//        List<KeyValuePair<IWindowed<string>, long>> expected = Arrays.asList(
//            KeyValuePair.Create(new Windowed<string>("a", new SessionWindow(0, 0)), 1L),
//            KeyValuePair.Create(new Windowed<string>("a", new SessionWindow(10, 10)), 2L),
//            KeyValuePair.Create(new Windowed<string>("a", new SessionWindow(100, 100)), 3L),
//            KeyValuePair.Create(new Windowed<string>("a", new SessionWindow(1000, 1000)), 4L));

//        foreach (KeyValuePair<IWindowed<string>, long> kv in expected)
//        {
//            sessionStore.Put(kv.Key, kv.Value);
//        }

//        IKeyValueIterator<IWindowed<string>, long> values = sessionStore.Fetch("a");
//        Assert.Equal(new HashSet<>(expected), toSet(values));

//        sessionStore.Close();

//        IKeyValueIterator<IWindowed<string>, long> values = sessionStore.Fetch("a");
//        Assert.Equal(Collections.emptySet(), toSet(values));

//        context.Restore(sessionStore.Name(), changeLog);

//        IKeyValueIterator<IWindowed<string>, long> values = sessionStore.Fetch("a");
//        Assert.Equal(new HashSet<>(expected), toSet(values));
//    }

//    [Fact]
//    public void ShouldCloseOpenIteratorsWhenStoreIsClosedAndNotThrowInvalidStateStoreExceptionOnHasNext()
//    {
//        sessionStore.Put(new Windowed<string>("a", new SessionWindow(0, 0)), 1L);
//        sessionStore.Put(new Windowed<string>("b", new SessionWindow(10, 50)), 2L);
//        sessionStore.Put(new Windowed<string>("c", new SessionWindow(100, 500)), 3L);

//        IKeyValueIterator<IWindowed<string>, long> iterator = sessionStore.Fetch("a");
//        Assert.True(iterator.MoveNext());
//        sessionStore.Close();

//        Assert.False(iterator.MoveNext());
//    }

//    [Fact]
//    public void ShouldReturnSameResultsForSingleKeyFindSessionsAndEqualKeyRangeFindSessions()
//    {
//        sessionStore.Put(new Windowed<string>("a", new SessionWindow(0, 1)), 0L);
//        sessionStore.Put(new Windowed<string>("aa", new SessionWindow(2, 3)), 1L);
//        sessionStore.Put(new Windowed<string>("aa", new SessionWindow(4, 5)), 2L);
//        sessionStore.Put(new Windowed<string>("aaa", new SessionWindow(6, 7)), 3L);

//        IKeyValueIterator<IWindowed<string>, long> singleKeyIterator = sessionStore.findSessions("aa", 0L, 10L);
//        IKeyValueIterator<IWindowed<string>, long> rangeIterator = sessionStore.findSessions("aa", "aa", 0L, 10L);

//        Assert.Equal(singleKeyIterator.MoveNext(), rangeIterator.MoveNext());
//        Assert.Equal(singleKeyIterator.MoveNext(), rangeIterator.MoveNext());
//        Assert.False(singleKeyIterator.MoveNext());
//        Assert.False(rangeIterator.MoveNext());
//    }

//    [Fact]
//    public void ShouldLogAndMeasureExpiredRecords()
//    {
//        setClassLoggerToDebug();
//        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

//        // Advance stream time by inserting record with large enough timestamp that records with timestamp 0 are expired
//        // Note that rocksdb will only expire segments at a time (where segment interval = 60,000 for this retention period)
//        sessionStore.Put(new Windowed<string>("initial record", new SessionWindow(0, 2 * SEGMENT_INTERVAL)), 0L);

//        // Try inserting a record with timestamp 0 -- should be dropped
//        sessionStore.Put(new Windowed<string>("late record", new SessionWindow(0, 0)), 0L);
//        sessionStore.Put(new Windowed<string>("another on-time record", new SessionWindow(0, 2 * SEGMENT_INTERVAL)), 0L);

//        LogCaptureAppender.Unregister(appender);

//        // Dictionary < MetricName, ? : Metric > metrics = context.metrics().metrics();

//        // string metricScope = getMetricsScope();

//        //Metric dropTotal = metrics.Get(new MetricName(
//        //    "expired-window-record-drop-total",
//        //    "stream-" + metricScope + "-metrics",
//        //    "The total number of occurrence of expired-window-record-drop operations.",
//        //    mkMap(
//        //        mkEntry("client-id", "mock"),
//        //        mkEntry("task-id", "0_0"),
//        //        mkEntry(metricScope + "-id", sessionStore.Name())
//        //    )
//        //));

//        //Metric dropRate = metrics.Get(new MetricName(
//        //    "expired-window-record-drop-rate",
//        //    "stream-" + metricScope + "-metrics",
//        //    "The average number of occurrence of expired-window-record-drop operation per second.",
//        //    mkMap(
//        //        mkEntry("client-id", "mock"),
//        //        mkEntry("task-id", "0_0"),
//        //        mkEntry(metricScope + "-id", sessionStore.Name())
//        //    )
//        //));

//        // Assert.Equal(1.0, dropTotal.metricValue());
//        //Assert.NotEqual(0.0, dropRate.metricValue());
//        List<string> messages = appender.getMessages();
//        Assert.Equal(messages, hasItem("Skipping record for expired segment."));
//    }

//    [Fact]
//    public void ShouldNotThrowExceptionRemovingNonexistentKey()
//    {
//        sessionStore.remove(new Windowed<string>("a", new SessionWindow(0, 1)));
//    }

//    [Fact]// (expected = NullReferenceException)
//    public void ShouldThrowNullPointerExceptionOnFindSessionsNullKey()
//    {
//        sessionStore.findSessions(null, 1L, 2L);
//    }

//    [Fact]// (expected = NullReferenceException)
//    public void ShouldThrowNullPointerExceptionOnFindSessionsNullFromKey()
//    {
//        sessionStore.findSessions(null, "anyKeyTo", 1L, 2L);
//    }

//    [Fact]// (expected = NullReferenceException)
//    public void ShouldThrowNullPointerExceptionOnFindSessionsNullToKey()
//    {
//        sessionStore.findSessions("anyKeyFrom", null, 1L, 2L);
//    }

//    [Fact]// (expected = NullReferenceException)
//    public void ShouldThrowNullPointerExceptionOnFetchNullFromKey()
//    {
//        sessionStore.Fetch(null, "anyToKey");
//    }

//    [Fact]// (expected = NullReferenceException)
//    public void ShouldThrowNullPointerExceptionOnFetchNullToKey()
//    {
//        sessionStore.Fetch("anyFromKey", null);
//    }

//    [Fact]// (expected = NullReferenceException)
//    public void ShouldThrowNullPointerExceptionOnFetchNullKey()
//    {
//        sessionStore.Fetch(null);
//    }

//    [Fact]// (expected = NullReferenceException)
//    public void ShouldThrowNullPointerExceptionOnRemoveNullKey()
//    {
//        sessionStore.remove(null);
//    }

//    [Fact]// (expected = NullReferenceException)
//    public void ShouldThrowNullPointerExceptionOnPutNullKey()
//    {
//        sessionStore.Put(null, 1L);
//    }

//    [Fact]
//    public void ShouldNotThrowInvalidRangeExceptionWithNegativeFromKey()
//    {
//        setClassLoggerToDebug();
//        LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

//        string keyFrom = Serdes.String().Deserializer
//            .Deserialize("", Serdes.Int().Serializer.Serialize("", -1));
//        string keyTo = Serdes.String().Deserializer
//            .Deserialize("", Serdes.Int().Serializer.Serialize("", 1));

//        IKeyValueIterator<IWindowed<string>, long> iterator = sessionStore.findSessions(keyFrom, keyTo, 0L, 10L);
//        Assert.False(iterator.MoveNext());

//        List<string> messages = appender.getMessages();
//        Assert.Equal(messages,
//            hasItem("Returning empty iterator for Fetch with invalid key range: from > to. "
//                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. "
//                + "Note that the built-in numerical serdes do not follow this for negative numbers"));
//    }

//    protected static HashSet<V> ValuesToSet<K, V>(Iterator<KeyValuePair<K, V>> iterator)
//    {
//        HashSet<V> results = new HashSet<V>();

//        while (iterator.MoveNext())
//        {
//            results.Add(iterator.MoveNext().Value);
//        }
//        return results;
//    }

//    protected static HashSet<KeyValuePair<K, V>> ToSet<K, V>(Iterator<KeyValuePair<K, V>> iterator)
//    {
//        HashSet<KeyValuePair<K, V>> results = new HashSet<KeyValuePair<K, V>>();

//        while (iterator.MoveNext())
//        {
//            results.Add(iterator.MoveNext());
//        }
//        return results;
//    }
//}
