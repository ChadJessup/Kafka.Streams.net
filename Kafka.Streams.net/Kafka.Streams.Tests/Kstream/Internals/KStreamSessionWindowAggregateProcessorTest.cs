namespace Kafka.Streams.Tests.Kstream.Internals
{
}
///*
//
//
//
//
//
//
// *
//
// *
//
//
//
//
//
// */
//using Kafka.Streams.Configs;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Tests.Helpers;
//using System;
//using System.IO;

//namespace Kafka.Streams.KStream.Internals
//{


















































//    public class KStreamSessionWindowAggregateProcessorTest
//    {

//        private static long GAP_MS = 5 * 60 * 1000L;
//        private static string STORE_NAME = "session-store";

//        private ToInternal toInternal = new ToInternal();
//        private Initializer<long> initializer = () => 0L;
//        private Aggregator<string, string, long> aggregator = (aggKey, value, aggregate) => aggregate + 1;
//        private Merger<string, long> sessionMerger = (aggKey, aggOne, aggTwo) => aggOne + aggTwo;
//        private KStreamSessionWindowAggregate<string, string, long> sessionAggregator =
//            new KStreamSessionWindowAggregate<>(
//                SessionWindows.with(TimeSpan.FromMilliseconds(GAP_MS)),
//                STORE_NAME,
//                initializer,
//                aggregator,
//                sessionMerger);

//        private List<KeyValueTimestamp> results = new List<>();
//        private Processor<string, string> processor = sessionAggregator.Get();
//        private ISessionStore<string, long> sessionStore;
//        private InternalMockProcessorContext context;
//        private Metrics metrics;


//        public void initializeStore()
//        {
//            File stateDir = TestUtils.GetTempDirectory();
//            metrics = new Metrics();
//            var metrics = new MockStreamsMetrics(KStreamSessionWindowAggregateProcessorTest.metrics);
//            ThreadMetrics.skipRecordSensor(metrics);

//            context = new InternalMockProcessorContext(
//                stateDir,
//                Serdes.String(),
//                Serdes.String(),
//                metrics,
//                new StreamsConfig(StreamsTestConfigs.GetStandardConfig()),
//                NoOpRecordCollector,
//                new ThreadCache(new LogContext("testCache "), 100000, metrics)
//            )
//            {


//            public void forward(K key, V value, To to)
//            {
//                toInternal.update(to);
//                results.Add(new KeyValueTimestamp<>(key, value, toInternal.Timestamp));
//            }
//        };

//        initStore(true);
//        processor.Init(context);
//    }

//    private void initStore(bool enableCaching)
//    {
//        IStoreBuilder<ISessionStore<string, long>> storeBuilder =
//            Stores.sessionStoreBuilder(
//                Stores.PersistentSessionStore(STORE_NAME, FromMilliseconds(GAP_MS * 3)),
//                Serdes.String(),
//                Serdes.Long())
//            .withLoggingDisabled();

//        if (enableCaching)
//        {
//            storeBuilder.withCachingEnabled();
//        }

//        sessionStore = storeBuilder.Build();
//        sessionStore.Init(context, sessionStore);
//    }


//    public void closeStore()
//    {
//        sessionStore.close();
//    }

//    [Fact]
//    public void shouldCreateSingleSessionWhenWithinGap()
//    {
//        context.setTime(0);
//        processor.process("john", "first");
//        context.setTime(500);
//        processor.process("john", "second");

//        IKeyValueIterator<Windowed<string>, long> values =
//            sessionStore.findSessions("john", 0, 2000);
//        Assert.True(values..AsNext());
//        Assert.Equal(long.valueOf(2), values.MoveNext().value);
//    }

//    [Fact]
//    public void shouldMergeSessions()
//    {
//        context.setTime(0);
//        var sessionId = "mel";
//        processor.process(sessionId, "first");
//        Assert.True(sessionStore.findSessions(sessionId, 0, 0)..AsNext());

//        // move time beyond gap
//        context.setTime(GAP_MS + 1);
//        processor.process(sessionId, "second");
//        Assert.True(sessionStore.findSessions(sessionId, GAP_MS + 1, GAP_MS + 1)..AsNext());
//        // should still exist.As not within gap
//        Assert.True(sessionStore.findSessions(sessionId, 0, 0)..AsNext());
//        // move time back
//        context.setTime(GAP_MS / 2);
//        processor.process(sessionId, "third");

//        IKeyValueIterator<Windowed<string>, long> iterator =
//            sessionStore.findSessions(sessionId, 0, GAP_MS + 1);
//        KeyValuePair<Windowed<string>, long> kv = iterator.MoveNext();

//        Assert.Equal(long.valueOf(3), kv.value);
//        Assert.False(iterator..AsNext());
//    }

//    [Fact]
//    public void shouldUpdateSessionIfTheSameTime()
//    {
//        context.setTime(0);
//        processor.process("mel", "first");
//        processor.process("mel", "second");
//        IKeyValueIterator<Windowed<string>, long> iterator =
//            sessionStore.findSessions("mel", 0, 0);
//        Assert.Equal(long.valueOf(2L), iterator.MoveNext().value);
//        Assert.False(iterator..AsNext());
//    }

//    [Fact]
//    public void shouldHaveMultipleSessionsForSameIdWhenTimestampApartBySessionGap()
//    {
//        var sessionId = "mel";
//        long time = 0;
//        context.setTime(time);
//        processor.process(sessionId, "first");
//        context.setTime(time += GAP_MS + 1);
//        processor.process(sessionId, "second");
//        processor.process(sessionId, "second");
//        context.setTime(time += GAP_MS + 1);
//        processor.process(sessionId, "third");
//        processor.process(sessionId, "third");
//        processor.process(sessionId, "third");

//        sessionStore.flush();
//        Assert.Equal(
//             Array.AsReadOnly(
//                 new KeyValueTimestamp<>(
//                     new Windowed<>(sessionId, new SessionWindow(0, 0)),
//                     new Change<>(1L, null),
//                     0L),
//                 new KeyValueTimestamp<>(
//                     new Windowed<>(sessionId, new SessionWindow(GAP_MS + 1, GAP_MS + 1)),
//                     new Change<>(2L, null),
//                     GAP_MS + 1),
//                 new KeyValueTimestamp<>(
//                     new Windowed<>(sessionId, new SessionWindow(time, time)),
//                     new Change<>(3L, null),
//                     time)
//             ),
//             results
//         );

//    }

//    [Fact]
//    public void shouldRemoveMergedSessionsFromStateStore()
//    {
//        context.setTime(0);
//        processor.process("a", "1");

//        // first ensure it is in the store
//        IKeyValueIterator<Windowed<string>, long> a1 =
//            sessionStore.findSessions("a", 0, 0);
//        Assert.Equal(KeyValuePair.Create(new Windowed<>("a", new SessionWindow(0, 0)), 1L), a1.MoveNext());

//        context.setTime(100);
//        processor.process("a", "2");
//        // a1 from above should have been removed
//        // should have merged session in store
//        IKeyValueIterator<Windowed<string>, long> a2 =
//            sessionStore.findSessions("a", 0, 100);
//        Assert.Equal(KeyValuePair.Create(new Windowed<>("a", new SessionWindow(0, 100)), 2L), a2.MoveNext());
//        Assert.False(a2..AsNext());
//    }

//    [Fact]
//    public void shouldHandleMultipleSessionsAndMerging()
//    {
//        context.setTime(0);
//        processor.process("a", "1");
//        processor.process("b", "1");
//        processor.process("c", "1");
//        processor.process("d", "1");
//        context.setTime(GAP_MS / 2);
//        processor.process("d", "2");
//        context.setTime(GAP_MS + 1);
//        processor.process("a", "2");
//        processor.process("b", "2");
//        context.setTime(GAP_MS + 1 + GAP_MS / 2);
//        processor.process("a", "3");
//        processor.process("c", "3");

//        sessionStore.flush();

//        Assert.Equal(
//             Array.AsReadOnly(
//                 new KeyValueTimestamp<>(
//                     new Windowed<>("a", new SessionWindow(0, 0)),
//                     new Change<>(1L, null),
//                     0L),
//                 new KeyValueTimestamp<>(
//                     new Windowed<>("b", new SessionWindow(0, 0)),
//                     new Change<>(1L, null),
//                     0L),
//                 new KeyValueTimestamp<>(
//                     new Windowed<>("c", new SessionWindow(0, 0)),
//                     new Change<>(1L, null),
//                     0L),
//                 new KeyValueTimestamp<>(
//                     new Windowed<>("d", new SessionWindow(0, GAP_MS / 2)),
//                     new Change<>(2L, null),
//                     GAP_MS / 2),
//                 new KeyValueTimestamp<>(
//                     new Windowed<>("b", new SessionWindow(GAP_MS + 1, GAP_MS + 1)),
//                     new Change<>(1L, null),
//                     GAP_MS + 1),
//                 new KeyValueTimestamp<>(
//                     new Windowed<>("a", new SessionWindow(GAP_MS + 1, GAP_MS + 1 + GAP_MS / 2)),
//                     new Change<>(2L, null),
//                     GAP_MS + 1 + GAP_MS / 2),
//                 new KeyValueTimestamp<>(new Windowed<>(
//                     "c",
//                     new SessionWindow(GAP_MS + 1 + GAP_MS / 2, GAP_MS + 1 + GAP_MS / 2)), new Change<>(1L, null),
//                     GAP_MS + 1 + GAP_MS / 2)
//             ),
//             results
//         );
//    }

//    [Fact]
//    public void shouldGetAggregatedValuesFromValueGetter()
//    {
//        KTableValueGetter<Windowed<string>, long> getter = sessionAggregator.view().Get();
//        getter.Init(context);
//        context.setTime(0);
//        processor.process("a", "1");
//        context.setTime(GAP_MS + 1);
//        processor.process("a", "1");
//        processor.process("a", "2");
//        long t0 = getter.Get(new Windowed<>("a", new SessionWindow(0, 0))).Value;
//        long t1 = getter.Get(new Windowed<>("a", new SessionWindow(GAP_MS + 1, GAP_MS + 1))).Value;
//        Assert.Equal(1L, t0);
//        Assert.Equal(2L, t1);
//    }

//    [Fact]
//    public void shouldImmediatelyForwardNewSessionWhenNonCachedStore()
//    {
//        initStore(false);
//        processor.Init(context);

//        context.setTime(0);
//        processor.process("a", "1");
//        processor.process("b", "1");
//        processor.process("c", "1");

//        Assert.Equal(
//             Array.AsReadOnly(
//                 new KeyValueTimestamp<>(
//                     new Windowed<>("a", new SessionWindow(0, 0)),
//                     new Change<>(1L, null),
//                     0L),
//                 new KeyValueTimestamp<>(
//                     new Windowed<>("b", new SessionWindow(0, 0)),
//                     new Change<>(1L, null),
//                     0L),
//                 new KeyValueTimestamp<>(
//                     new Windowed<>("c", new SessionWindow(0, 0)),
//                     new Change<>(1L, null),
//                     0L)
//             ),
//             results
//         );
//    }

//    [Fact]
//    public void shouldImmediatelyForwardRemovedSessionsWhenMerging()
//    {
//        initStore(false);
//        processor.Init(context);

//        context.setTime(0);
//        processor.process("a", "1");
//        context.setTime(5);
//        processor.process("a", "1");
//        Assert.Equal(
//             Array.AsReadOnly(
//                 new KeyValueTimestamp<>(
//                     new Windowed<>("a", new SessionWindow(0, 0)),
//                     new Change<>(1L, null),
//                     0L),
//                 new KeyValueTimestamp<>(
//                     new Windowed<>("a", new SessionWindow(0, 0)),
//                     new Change<>(null, null),
//                     0L),
//                 new KeyValueTimestamp<>(
//                     new Windowed<>("a", new SessionWindow(0, 5)),
//                     new Change<>(2L, null),
//                     5L)
//             ),
//             results
//         );

//    }

//    [Fact]
//    public void shouldLogAndMeterWhenSkippingNullKey()
//    {
//        initStore(false);
//        processor.Init(context);
//        context.setRecordContext(new ProcessorRecordContext(-1, -2, -3, "topic", null));
//        LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//        processor.process(null, "1");
//        LogCaptureAppender.unregister(appender);

//        Assert.Equal(
//             1.0,
//             getMetricByName(context.metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
//        Assert.Equal(
//            appender.getMessages(),
//            asItem("Skipping record due to null key. value=[1] topic=[topic] partition=[-3] offset=[-2]"));
//    }

//    [Fact]
//    public void shouldLogAndMeterWhenSkippingLateRecordWithZeroGrace()
//    {
//        LogCaptureAppender.setClassLoggerToDebug(KStreamSessionWindowAggregate));
//        LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//        Processor<string, string> processor = new KStreamSessionWindowAggregate<>(
//            SessionWindows.with(TimeSpan.FromMilliseconds(10L)).grace(TimeSpan.FromMilliseconds(0L)),
//            STORE_NAME,
//            initializer,
//            aggregator,
//            sessionMerger
//        ).Get();

//        initStore(false);
//        processor.Init(context);

//        // dummy record to establish stream time = 0
//        context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
//        processor.process("dummy", "dummy");

//        // record arrives on time, should not be skipped
//        context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
//        processor.process("OnTime1", "1");

//        // dummy record to advance stream time = 1
//        context.setRecordContext(new ProcessorRecordContext(1, -2, -3, "topic", null));
//        processor.process("dummy", "dummy");

//        // record is late
//        context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
//        processor.process("Late1", "1");
//        LogCaptureAppender.unregister(appender);

//        var dropMetric = new MetricName(
//            "late-record-drop-total",
//            "stream-processor-node-metrics",
//            "The total number of occurrence of late-record-drop operations.",
//            mkMap(
//                mkEntry("client-id", "test"),
//                mkEntry("Task-id", "0_0"),
//                mkEntry("processor-node-id", "TESTING_NODE")
//            )
//        );

//        Assert.Equal(metrics.metrics().Get(dropMetric).metricValue(), (1.0));

//        var dropRate = new MetricName(
//            "late-record-drop-rate",
//            "stream-processor-node-metrics",
//            "The average number of occurrence of late-record-drop operations.",
//            mkMap(
//                mkEntry("client-id", "test"),
//                mkEntry("Task-id", "0_0"),
//                mkEntry("processor-node-id", "TESTING_NODE")
//            )
//        );

//        Assert.Equal(
//            (double)metrics.metrics().Get(dropRate).metricValue(),
//            greaterThan(0.0));
//        Assert.Equal(
//            appender.getMessages(),
//            asItem("Skipping record for expired window. key=[Late1] topic=[topic] partition=[-3] offset=[-2] timestamp=[0] window=[0,0] expiration=[1] streamTime=[1]"));
//    }

//    [Fact]
//    public void shouldLogAndMeterWhenSkippingLateRecordWithNonzeroGrace()
//    {
//        LogCaptureAppender.setClassLoggerToDebug(KStreamSessionWindowAggregate));
//        LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
//        Processor<string, string> processor = new KStreamSessionWindowAggregate<>(
//            SessionWindows.with(TimeSpan.FromMilliseconds(10L)).grace(TimeSpan.FromMilliseconds(1L)),
//            STORE_NAME,
//            initializer,
//            aggregator,
//            sessionMerger
//        ).Get();

//        initStore(false);
//        processor.Init(context);

//        // dummy record to establish stream time = 0
//        context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
//        processor.process("dummy", "dummy");

//        // record arrives on time, should not be skipped
//        context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
//        processor.process("OnTime1", "1");

//        // dummy record to advance stream time = 1
//        context.setRecordContext(new ProcessorRecordContext(1, -2, -3, "topic", null));
//        processor.process("dummy", "dummy");

//        // delayed record arrives on time, should not be skipped
//        context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
//        processor.process("OnTime2", "1");

//        // dummy record to advance stream time = 2
//        context.setRecordContext(new ProcessorRecordContext(2, -2, -3, "topic", null));
//        processor.process("dummy", "dummy");

//        // delayed record arrives late
//        context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
//        processor.process("Late1", "1");


//        LogCaptureAppender.unregister(appender);

//        var dropMetric = new MetricName(
//            "late-record-drop-total",
//            "stream-processor-node-metrics",
//            "The total number of occurrence of late-record-drop operations.",
//            mkMap(
//                mkEntry("client-id", "test"),
//                mkEntry("Task-id", "0_0"),
//                mkEntry("processor-node-id", "TESTING_NODE")
//            )
//        );

//        Assert.Equal(metrics.metrics().Get(dropMetric).metricValue(), (1.0));

//        var dropRate = new MetricName(
//            "late-record-drop-rate",
//            "stream-processor-node-metrics",
//            "The average number of occurrence of late-record-drop operations.",
//            mkMap(
//                mkEntry("client-id", "test"),
//                mkEntry("Task-id", "0_0"),
//                mkEntry("processor-node-id", "TESTING_NODE")
//            )
//        );

//        Assert.Equal(
//            (double)metrics.metrics().Get(dropRate).metricValue(),
//            greaterThan(0.0));
//        Assert.Equal(
//            appender.getMessages(),
//            asItem("Skipping record for expired window. key=[Late1] topic=[topic] partition=[-3] offset=[-2] timestamp=[0] window=[0,0] expiration=[1] streamTime=[2]"));
//    }
//}
