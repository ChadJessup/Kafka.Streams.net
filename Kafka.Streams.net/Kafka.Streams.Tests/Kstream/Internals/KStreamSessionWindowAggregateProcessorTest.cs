using Kafka.Streams.Configs;
using Kafka.Streams.KStream;
using Kafka.Streams.Tests.Helpers;
using System;
using System.IO;

namespace Kafka.Streams.Tests.Kstream.Internals
{


















































    public class KStreamSessionWindowAggregateProcessorTest
    {

        private static long GAP_MS = 5 * 60 * 1000L;
        private static string STORE_NAME = "session-store";

        private ToInternal toInternal = new ToInternal();
        private WrappedInitializer<long> initializer = () => 0L;
        private WrappedAggregator<string, string, long> aggregator = (aggKey, value, aggregate) => aggregate + 1;
        private Merger<string, long> sessionMerger = (aggKey, aggOne, aggTwo) => aggOne + aggTwo;
        private KStreamSessionWindowAggregate<string, string, long> sessionAggregator =
            new KStreamSessionWindowAggregate<>(
                SessionWindows.With(TimeSpan.FromMilliseconds(GAP_MS)),
                STORE_NAME,
                initializer,
                aggregator,
                sessionMerger);

        private List<KeyValueTimestamp> results = new List<>();
        private Processor<string, string> processor = sessionAggregator.Get();
        private ISessionStore<string, long> sessionStore;
        private InternalMockProcessorContext context;
        private Metrics metrics;


        public void initializeStore()
        {
            var stateDir = TestUtils.GetTempDirectory();
            //metrics = new Metrics();
            //var metrics = new MockStreamsMetrics(KStreamSessionWindowAggregateProcessorTest.metrics);
            //ThreadMetrics.skipRecordSensor(metrics);

            context = new InternalMockProcessorContext(
                stateDir,
                Serdes.String(),
                Serdes.String(),
                metrics,
                new StreamsConfig(StreamsTestConfigs.GetStandardConfig()),
                NoOpRecordCollector,
                new ThreadCache(new LogContext("testCache "), 100000, metrics));
            //            {
            //
            //
            //            public void forward(K key, V value, To to)
            //            {
            //                toInternal.Update(to);
            //                results.Add(new KeyValueTimestamp<>(key, value, toInternal.Timestamp));
            //            }
            //        };

            initStore(true);
            processor.Init(context);
        }

        private void initStore(bool enableCaching)
        {
            IStoreBuilder<ISessionStore<string, long>> storeBuilder =
                Stores.sessionStoreBuilder(
                    Stores.PersistentSessionStore(STORE_NAME, FromMilliseconds(GAP_MS * 3)),
                    Serdes.String(),
                    Serdes.Long())
                .WithLoggingDisabled();

            if (enableCaching)
            {
                storeBuilder.withCachingEnabled();
            }

            sessionStore = storeBuilder.Build();
            sessionStore.Init(context, sessionStore);
        }


        public void closeStore()
        {
            sessionStore.Close();
        }

        [Fact]
        public void shouldCreateSingleSessionWhenWithinGap()
        {
            context.setTime(0);
            processor.Process("john", "first");
            context.setTime(500);
            processor.Process("john", "second");

            IKeyValueIterator<IWindowed<string>, long> values =
                sessionStore.findSessions("john", 0, 2000);
            Assert.True(values..AsNext());
            Assert.Equal(long.valueOf(2), values.MoveNext().Value);
        }

        [Fact]
        public void shouldMergeSessions()
        {
            context.setTime(0);
            var sessionId = "mel";
            processor.Process(sessionId, "first");
            Assert.True(sessionStore.findSessions(sessionId, 0, 0)..AsNext());

            // move time beyond gap
            context.setTime(GAP_MS + 1);
            processor.Process(sessionId, "second");
            Assert.True(sessionStore.findSessions(sessionId, GAP_MS + 1, GAP_MS + 1)..AsNext());
            // should still exist.As not within gap
            Assert.True(sessionStore.findSessions(sessionId, 0, 0)..AsNext());
            // move time back
            context.setTime(GAP_MS / 2);
            processor.Process(sessionId, "third");

            IKeyValueIterator<IWindowed<string>, long> iterator =
                sessionStore.findSessions(sessionId, 0, GAP_MS + 1);
            KeyValuePair<IWindowed<string>, long> kv = iterator.MoveNext();

            Assert.Equal(long.valueOf(3), kv.Value);
            Assert.False(iterator..AsNext());
        }

        [Fact]
        public void shouldUpdateSessionIfTheSameTime()
        {
            context.setTime(0);
            processor.Process("mel", "first");
            processor.Process("mel", "second");
            IKeyValueIterator<IWindowed<string>, long> iterator =
                sessionStore.findSessions("mel", 0, 0);
            Assert.Equal(long.valueOf(2L), iterator.MoveNext().Value);
            Assert.False(iterator..AsNext());
        }

        [Fact]
        public void shouldHaveMultipleSessionsForSameIdWhenTimestampApartBySessionGap()
        {
            var sessionId = "mel";
            long time = 0;
            context.setTime(time);
            processor.Process(sessionId, "first");
            context.setTime(time += GAP_MS + 1);
            processor.Process(sessionId, "second");
            processor.Process(sessionId, "second");
            context.setTime(time += GAP_MS + 1);
            processor.Process(sessionId, "third");
            processor.Process(sessionId, "third");
            processor.Process(sessionId, "third");

            sessionStore.Flush();
            Assert.Equal(
                 Array.AsReadOnly(
                     new KeyValueTimestamp<>(
                         new Windowed<string>(sessionId, new SessionWindow(0, 0)),
                         new Change<>(1L, null),
                         0L),
                     new KeyValueTimestamp<>(
                         new Windowed<string>(sessionId, new SessionWindow(GAP_MS + 1, GAP_MS + 1)),
                         new Change<>(2L, null),
                         GAP_MS + 1),
                     new KeyValueTimestamp<>(
                         new Windowed<string>(sessionId, new SessionWindow(time, time)),
                         new Change<>(3L, null),
                         time)
                 ),
                 results
             );

        }

        [Fact]
        public void shouldRemoveMergedSessionsFromStateStore()
        {
            context.setTime(0);
            processor.Process("a", "1");

            // first ensure it is in the store
            IKeyValueIterator<IWindowed<string>, long> a1 =
                sessionStore.findSessions("a", 0, 0);
            Assert.Equal(KeyValuePair.Create(new Windowed<string>("a", new SessionWindow(0, 0)), 1L), a1.MoveNext());

            context.setTime(100);
            processor.Process("a", "2");
            // a1 from above should have been removed
            // should have merged session in store
            IKeyValueIterator<IWindowed<string>, long> a2 =
                sessionStore.findSessions("a", 0, 100);
            Assert.Equal(KeyValuePair.Create(new Windowed<string>("a", new SessionWindow(0, 100)), 2L), a2.MoveNext());
            Assert.False(a2..AsNext());
        }

        [Fact]
        public void shouldHandleMultipleSessionsAndMerging()
        {
            context.setTime(0);
            processor.Process("a", "1");
            processor.Process("b", "1");
            processor.Process("c", "1");
            processor.Process("d", "1");
            context.setTime(GAP_MS / 2);
            processor.Process("d", "2");
            context.setTime(GAP_MS + 1);
            processor.Process("a", "2");
            processor.Process("b", "2");
            context.setTime(GAP_MS + 1 + GAP_MS / 2);
            processor.Process("a", "3");
            processor.Process("c", "3");

            sessionStore.Flush();

            Assert.Equal(
                 Array.AsReadOnly(
                     new KeyValueTimestamp<>(
                         new Windowed<string>("a", new SessionWindow(0, 0)),
                         new Change<>(1L, null),
                         0L),
                     new KeyValueTimestamp<>(
                         new Windowed<string>("b", new SessionWindow(0, 0)),
                         new Change<>(1L, null),
                         0L),
                     new KeyValueTimestamp<>(
                         new Windowed<string>("c", new SessionWindow(0, 0)),
                         new Change<>(1L, null),
                         0L),
                     new KeyValueTimestamp<>(
                         new Windowed<string>("d", new SessionWindow(0, GAP_MS / 2)),
                         new Change<>(2L, null),
                         GAP_MS / 2),
                     new KeyValueTimestamp<>(
                         new Windowed<string>("b", new SessionWindow(GAP_MS + 1, GAP_MS + 1)),
                         new Change<>(1L, null),
                         GAP_MS + 1),
                     new KeyValueTimestamp<>(
                         new Windowed<string>("a", new SessionWindow(GAP_MS + 1, GAP_MS + 1 + GAP_MS / 2)),
                         new Change<>(2L, null),
                         GAP_MS + 1 + GAP_MS / 2),
                     new KeyValueTimestamp<>(new Windowed<string>(
                         "c",
                         new SessionWindow(GAP_MS + 1 + GAP_MS / 2, GAP_MS + 1 + GAP_MS / 2)), new Change<>(1L, null),
                         GAP_MS + 1 + GAP_MS / 2)
                 ),
                 results
             );
        }

        [Fact]
        public void shouldGetAggregatedValuesFromValueGetter()
        {
            KTableValueGetter<IWindowed<string>, long> getter = sessionAggregator.view().Get();
            getter.Init(context);
            context.setTime(0);
            processor.Process("a", "1");
            context.setTime(GAP_MS + 1);
            processor.Process("a", "1");
            processor.Process("a", "2");
            long t0 = getter.Get(new Windowed<string>("a", new SessionWindow(0, 0))).Value;
            long t1 = getter.Get(new Windowed<string>("a", new SessionWindow(GAP_MS + 1, GAP_MS + 1))).Value;
            Assert.Equal(1L, t0);
            Assert.Equal(2L, t1);
        }

        [Fact]
        public void shouldImmediatelyForwardNewSessionWhenNonCachedStore()
        {
            initStore(false);
            processor.Init(context);

            context.setTime(0);
            processor.Process("a", "1");
            processor.Process("b", "1");
            processor.Process("c", "1");

            Assert.Equal(
                 Array.AsReadOnly(
                     new KeyValueTimestamp<>(
                         new Windowed<string>("a", new SessionWindow(0, 0)),
                         new Change<>(1L, null),
                         0L),
                     new KeyValueTimestamp<>(
                         new Windowed<string>("b", new SessionWindow(0, 0)),
                         new Change<>(1L, null),
                         0L),
                     new KeyValueTimestamp<>(
                         new Windowed<string>("c", new SessionWindow(0, 0)),
                         new Change<>(1L, null),
                         0L)
                 ),
                 results
             );
        }

        [Fact]
        public void shouldImmediatelyForwardRemovedSessionsWhenMerging()
        {
            initStore(false);
            processor.Init(context);

            context.setTime(0);
            processor.Process("a", "1");
            context.setTime(5);
            processor.Process("a", "1");
            Assert.Equal(
                 Array.AsReadOnly(
                     new KeyValueTimestamp<>(
                         new Windowed<string>("a", new SessionWindow(0, 0)),
                         new Change<>(1L, null),
                         0L),
                     new KeyValueTimestamp<>(
                         new Windowed<string>("a", new SessionWindow(0, 0)),
                         new Change<>(null, null),
                         0L),
                     new KeyValueTimestamp<>(
                         new Windowed<string>("a", new SessionWindow(0, 5)),
                         new Change<>(2L, null),
                         5L)
                 ),
                 results
             );

        }

        [Fact]
        public void shouldLogAndMeterWhenSkippingNullKey()
        {
            initStore(false);
            processor.Init(context);
            context.setRecordContext(new ProcessorRecordContext(-1, -2, -3, "topic", null));
            LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
            processor.Process(null, "1");
            LogCaptureAppender.unregister(appender);

            Assert.Equal(
                 1.0,
                 getMetricByName(context.metrics().metrics(), "skipped-records-total", "stream-metrics").metricValue());
            Assert.Equal(
                appender.getMessages(),
                asItem("Skipping record due to null key. value=[1] topic=[topic] partition=[-3] offset=[-2]"));
        }

        [Fact]
        public void shouldLogAndMeterWhenSkippingLateRecordWithZeroGrace()
        {
            LogCaptureAppender.setClassLoggerToDebug(KStreamSessionWindowAggregate));
            LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
            Processor<string, string> processor = new KStreamSessionWindowAggregate<>(
                SessionWindows.With(TimeSpan.FromMilliseconds(10L)).Grace(TimeSpan.FromMilliseconds(0L)),
                STORE_NAME,
                initializer,
                aggregator,
                sessionMerger
            ).Get();

            initStore(false);
            processor.Init(context);

            // dummy record to establish stream time = 0
            context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
            processor.Process("dummy", "dummy");

            // record arrives on time, should not be skipped
            context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
            processor.Process("OnTime1", "1");

            // dummy record to advance stream time = 1
            context.setRecordContext(new ProcessorRecordContext(1, -2, -3, "topic", null));
            processor.Process("dummy", "dummy");

            // record is late
            context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
            processor.Process("Late1", "1");
            LogCaptureAppender.unregister(appender);

            var dropMetric = new MetricName(
                "late-record-drop-total",
                "stream-processor-node-metrics",
                "The total number of occurrence of late-record-drop operations.",
                mkMap(
                    mkEntry("client-id", "test"),
                    mkEntry("Task-id", "0_0"),
                    mkEntry("processor-node-id", "TESTING_NODE")
                )
            );

            Assert.Equal(metrics.metrics().Get(dropMetric).metricValue(), 1.0);

            var dropRate = new MetricName(
                "late-record-drop-rate",
                "stream-processor-node-metrics",
                "The average number of occurrence of late-record-drop operations.",
                mkMap(
                    mkEntry("client-id", "test"),
                    mkEntry("Task-id", "0_0"),
                    mkEntry("processor-node-id", "TESTING_NODE")
                )
            );

            Assert.Equal(
                (double)metrics.metrics().Get(dropRate).metricValue(),
                greaterThan(0.0));
            Assert.Equal(
                appender.getMessages(),
                asItem("Skipping record for expired window. key=[Late1] topic=[topic] partition=[-3] offset=[-2] timestamp=[0] window=[0,0] expiration=[1] streamTime=[1]"));
        }

        [Fact]
        public void shouldLogAndMeterWhenSkippingLateRecordWithNonzeroGrace()
        {
            LogCaptureAppender.setClassLoggerToDebug(KStreamSessionWindowAggregate));
            LogCaptureAppender appender = LogCaptureAppender.createAndRegister();
            Processor<string, string> processor = new KStreamSessionWindowAggregate<>(
                SessionWindows.With(TimeSpan.FromMilliseconds(10L)).Grace(TimeSpan.FromMilliseconds(1L)),
                STORE_NAME,
                initializer,
                aggregator,
                sessionMerger
            ).Get();

            initStore(false);
            processor.Init(context);

            // dummy record to establish stream time = 0
            context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
            processor.Process("dummy", "dummy");

            // record arrives on time, should not be skipped
            context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
            processor.Process("OnTime1", "1");

            // dummy record to advance stream time = 1
            context.setRecordContext(new ProcessorRecordContext(1, -2, -3, "topic", null));
            processor.Process("dummy", "dummy");

            // delayed record arrives on time, should not be skipped
            context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
            processor.Process("OnTime2", "1");

            // dummy record to advance stream time = 2
            context.setRecordContext(new ProcessorRecordContext(2, -2, -3, "topic", null));
            processor.Process("dummy", "dummy");

            // delayed record arrives late
            context.setRecordContext(new ProcessorRecordContext(0, -2, -3, "topic", null));
            processor.Process("Late1", "1");


            LogCaptureAppender.unregister(appender);

            var dropMetric = new MetricName(
                "late-record-drop-total",
                "stream-processor-node-metrics",
                "The total number of occurrence of late-record-drop operations.",
                mkMap(
                    mkEntry("client-id", "test"),
                    mkEntry("Task-id", "0_0"),
                    mkEntry("processor-node-id", "TESTING_NODE")
                )
            );

            Assert.Equal(metrics.metrics().Get(dropMetric).metricValue(), 1.0);

            var dropRate = new MetricName(
                "late-record-drop-rate",
                "stream-processor-node-metrics",
                "The average number of occurrence of late-record-drop operations.",
                mkMap(
                    mkEntry("client-id", "test"),
                    mkEntry("Task-id", "0_0"),
                    mkEntry("processor-node-id", "TESTING_NODE")
                )
            );

            Assert.Equal(
                (double)metrics.metrics().Get(dropRate).metricValue(),
                greaterThan(0.0));
            Assert.Equal(
                appender.getMessages(),
                asItem("Skipping record for expired window. key=[Late1] topic=[topic] partition=[-3] offset=[-2] timestamp=[0] window=[0,0] expiration=[1] streamTime=[2]"));
        }
    }
