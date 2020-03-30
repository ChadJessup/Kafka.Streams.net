namespace Kafka.Streams.Tests.State.Internals
{
    /*






    *

    *





    */
























































    public abstract class AbstractRocksDBSegmentedBytesStoreTest<S : Segment> {

        private readonly long windowSizeForTimeWindow = 500;
        private InternalMockProcessorContext context;
        private AbstractRocksDBSegmentedBytesStore<S> bytesStore;
        private File stateDir;
        private readonly Window[] windows = new Window[4];
        private Window nextSegmentWindow;
        readonly long retention = 1000;
        readonly long segmentInterval = 60_000L;
        readonly string storeName = "bytes-store";

        @Parameter
        public SegmentedBytesStore.KeySchema schema;

        @Parameters(name = "{0}")
        public static object[] GetKeySchemas()
        {
            return new object[] { new SessionKeySchema(), new WindowKeySchema() };
        }


        public void Before()
        {
            if (schema is SessionKeySchema)
            {
                windows[0] = new SessionWindow(10L, 10L);
                windows[1] = new SessionWindow(500L, 1000L);
                windows[2] = new SessionWindow(1_000L, 1_500L);
                windows[3] = new SessionWindow(30_000L, 60_000L);
                // All four of the previous windows will go into segment 1.
                // The nextSegmentWindow is computed be a high enough time that when it gets written
                // to the segment store, it will advance stream time past the first segment's retention time and
                // expire it.
                nextSegmentWindow = new SessionWindow(segmentInterval + retention, segmentInterval + retention);
            }
            if (schema is WindowKeySchema)
            {
                windows[0] = timeWindowForSize(10L, windowSizeForTimeWindow);
                windows[1] = timeWindowForSize(500L, windowSizeForTimeWindow);
                windows[2] = timeWindowForSize(1_000L, windowSizeForTimeWindow);
                windows[3] = timeWindowForSize(60_000L, windowSizeForTimeWindow);
                // All four of the previous windows will go into segment 1.
                // The nextSegmentWindow is computed be a high enough time that when it gets written
                // to the segment store, it will advance stream time past the first segment's retention time and
                // expire it.
                nextSegmentWindow = timeWindowForSize(segmentInterval + retention, windowSizeForTimeWindow);
            }

            bytesStore = GetBytesStore();

            stateDir = TestUtils.tempDirectory();
            context = new InternalMockProcessorContext(
                stateDir,
                Serdes.String(),
                Serdes.Long(),
                new NoOpRecordCollector(),
                new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
            );
            bytesStore.init(context, bytesStore);
        }


        public void Close()
        {
            bytesStore.close();
        }

        abstract AbstractRocksDBSegmentedBytesStore<S> GetBytesStore();

        abstract AbstractSegments<S> NewSegments();

        abstract Options GetOptions(S segment);

        [Xunit.Fact]
        public void ShouldPutAndFetch()
        {
            string key = "a";
            bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), SerializeValue(10));
            bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), SerializeValue(50));
            bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), SerializeValue(100));

            KeyValueIterator<Bytes, byte[]> values = bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 500);

            List<KeyValuePair<Windowed<string>, long>> expected = Array.asList(
                KeyValuePair.Create(new Windowed<>(key, windows[0]), 10L),
                KeyValuePair.Create(new Windowed<>(key, windows[1]), 50L)
            );

            Assert.Equal(expected, ToList(values));
        }

        [Xunit.Fact]
        public void ShouldFindValuesWithinRange()
        {
            string key = "a";
            bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), SerializeValue(10));
            bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), SerializeValue(50));
            bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), SerializeValue(100));
            KeyValueIterator<Bytes, byte[]> results = bytesStore.fetch(Bytes.wrap(key.getBytes()), 1, 999);
            List<KeyValuePair<Windowed<string>, long>> expected = Array.asList(
                KeyValuePair.Create(new Windowed<>(key, windows[0]), 10L),
                KeyValuePair.Create(new Windowed<>(key, windows[1]), 50L)
            );

            Assert.Equal(expected, ToList(results));
        }

        [Xunit.Fact]
        public void ShouldRemove()
        {
            bytesStore.put(serializeKey(new Windowed<>("a", windows[0])), SerializeValue(30));
            bytesStore.put(serializeKey(new Windowed<>("a", windows[1])), SerializeValue(50));

            bytesStore.remove(serializeKey(new Windowed<>("a", windows[0])));
            KeyValueIterator<Bytes, byte[]> value = bytesStore.fetch(Bytes.wrap("a".getBytes()), 0, 100);
            Assert.False(value.hasNext());
        }

        [Xunit.Fact]
        public void ShouldRollSegments()
        {
            // just to validate directories
            AbstractSegments<S> segments = NewSegments();
            string key = "a";

            bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), SerializeValue(50));
            bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), SerializeValue(100));
            bytesStore.put(serializeKey(new Windowed<>(key, windows[2])), SerializeValue(500));
            Assert.Equal(Collections.singleton(segments.segmentName(0)), SegmentDirs());

            bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), SerializeValue(1000));
            Assert.Equal(Utils.mkSet(segments.segmentName(0), segments.segmentName(1)), SegmentDirs());

            List<KeyValuePair<Windowed<string>, long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0, 1500));

            Assert.Equal(
                Array.asList(
                    KeyValuePair.Create(new Windowed<>(key, windows[0]), 50L),
                    KeyValuePair.Create(new Windowed<>(key, windows[1]), 100L),
                    KeyValuePair.Create(new Windowed<>(key, windows[2]), 500L)
                ),
                results
            );
        }

        [Xunit.Fact]
        public void ShouldGetAllSegments()
        {
            // just to validate directories
            AbstractSegments<S> segments = NewSegments();
            string key = "a";

            bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), SerializeValue(50L));
            Assert.Equal(Collections.singleton(segments.segmentName(0)), SegmentDirs());

            bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), SerializeValue(100L));
            Assert.Equal(
                Utils.mkSet(
                    segments.segmentName(0),
                    segments.segmentName(1)
                ),
                SegmentDirs()
            );

            List<KeyValuePair<Windowed<string>, long>> results = toList(bytesStore.all());
            Assert.Equal(
                Array.asList(
                    KeyValuePair.Create(new Windowed<>(key, windows[0]), 50L),
                    KeyValuePair.Create(new Windowed<>(key, windows[3]), 100L)
                ),
                results
            );
        }

        [Xunit.Fact]
        public void ShouldFetchAllSegments()
        {
            // just to validate directories
            AbstractSegments<S> segments = NewSegments();
            string key = "a";

            bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), SerializeValue(50L));
            Assert.Equal(Collections.singleton(segments.segmentName(0)), SegmentDirs());

            bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), SerializeValue(100L));
            Assert.Equal(
                Utils.mkSet(
                    segments.segmentName(0),
                    segments.segmentName(1)
                ),
                SegmentDirs()
            );

            List<KeyValuePair<Windowed<string>, long>> results = toList(bytesStore.fetchAll(0L, 60_000L));
            Assert.Equal(
                Array.asList(
                    KeyValuePair.Create(new Windowed<>(key, windows[0]), 50L),
                    KeyValuePair.Create(new Windowed<>(key, windows[3]), 100L)
                ),
                results
            );
        }

        [Xunit.Fact]
        public void ShouldLoadSegmentsWithOldStyleDateFormattedName()
        {
            AbstractSegments<S> segments = NewSegments();
            string key = "a";

            bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), SerializeValue(50L));
            bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), SerializeValue(100L));
            bytesStore.close();

            string firstSegmentName = segments.segmentName(0);
            string[] nameParts = firstSegmentName.split("\\.");
            long segmentId = long.parseLong(nameParts[1]);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
            formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
            string formatted = formatter.format(new Date(segmentId * segmentInterval));
            File parent = new File(stateDir, storeName);
            File oldStyleName = new File(parent, nameParts[0] + "-" + formatted);
            Assert.True(new File(parent, firstSegmentName).renameTo(oldStyleName));

            bytesStore = GetBytesStore();

            bytesStore.init(context, bytesStore);
            List<KeyValuePair<Windowed<string>, long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60_000L));
            Assert.Equal(
                results,
                equalTo(
                    Array.asList(
                        KeyValuePair.Create(new Windowed<>(key, windows[0]), 50L),
                        KeyValuePair.Create(new Windowed<>(key, windows[3]), 100L)
                    )
                )
            );
        }

        [Xunit.Fact]
        public void ShouldLoadSegmentsWithOldStyleColonFormattedName()
        {
            AbstractSegments<S> segments = NewSegments();
            string key = "a";

            bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), SerializeValue(50L));
            bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), SerializeValue(100L));
            bytesStore.close();

            string firstSegmentName = segments.segmentName(0);
            string[] nameParts = firstSegmentName.split("\\.");
            File parent = new File(stateDir, storeName);
            File oldStyleName = new File(parent, nameParts[0] + ":" + long.parseLong(nameParts[1]));
            Assert.True(new File(parent, firstSegmentName).renameTo(oldStyleName));

            bytesStore = GetBytesStore();

            bytesStore.init(context, bytesStore);
            List<KeyValuePair<Windowed<string>, long>> results = toList(bytesStore.fetch(Bytes.wrap(key.getBytes()), 0L, 60_000L));
            Assert.Equal(
                results,
                equalTo(
                    Array.asList(
                        KeyValuePair.Create(new Windowed<>(key, windows[0]), 50L),
                        KeyValuePair.Create(new Windowed<>(key, windows[3]), 100L)
                    )
                )
            );
        }

        [Xunit.Fact]
        public void ShouldBeAbleToWriteToReInitializedStore()
        {
            string key = "a";
            // need to create a segment so we can attempt to write to it again.
            bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), SerializeValue(50));
            bytesStore.close();
            bytesStore.init(context, bytesStore);
            bytesStore.put(serializeKey(new Windowed<>(key, windows[1])), SerializeValue(100));
        }

        [Xunit.Fact]
        public void ShouldCreateWriteBatches()
        {
            string key = "a";
            Collection<KeyValuePair<byte[], byte[]>> records = new ArrayList<>();
            records.add(new KeyValuePair<>(serializeKey(new Windowed<>(key, windows[0])).get(), SerializeValue(50L)));
            records.add(new KeyValuePair<>(serializeKey(new Windowed<>(key, windows[3])).get(), SerializeValue(100L)));
            Dictionary<S, WriteBatch> writeBatchMap = bytesStore.getWriteBatches(records);
            Assert.Equal(2, writeBatchMap.Count);
            foreach (WriteBatch batch in writeBatchMap.values())
            {
                Assert.Equal(1, batch.count());
            }
        }

        [Xunit.Fact]
        public void ShouldRestoreToByteStore()
        {
            // 0 segments initially.
            Assert.Equal(0, bytesStore.getSegments().Count);
            string key = "a";
            Collection<KeyValuePair<byte[], byte[]>> records = new ArrayList<>();
            records.add(new KeyValuePair<>(serializeKey(new Windowed<>(key, windows[0])).get(), SerializeValue(50L)));
            records.add(new KeyValuePair<>(serializeKey(new Windowed<>(key, windows[3])).get(), SerializeValue(100L)));
            bytesStore.restoreAllInternal(records);

            // 2 segments are created during restoration.
            Assert.Equal(2, bytesStore.getSegments().Count);

            // Bulk loading is enabled during recovery.
            foreach (S segment in bytesStore.getSegments())
            {
                Assert.Equal(GetOptions(segment).level0FileNumCompactionTrigger(), (1 << 30));
            }

            List<KeyValuePair<Windowed<string>, long>> expected = new ArrayList<>();
            expected.add(new KeyValuePair<>(new Windowed<>(key, windows[0]), 50L));
            expected.add(new KeyValuePair<>(new Windowed<>(key, windows[3]), 100L));

            List<KeyValuePair<Windowed<string>, long>> results = toList(bytesStore.all());
            Assert.Equal(expected, results);
        }

        [Xunit.Fact]
        public void ShouldRespectBulkLoadOptionsDuringInit()
        {
            bytesStore.init(context, bytesStore);
            string key = "a";
            bytesStore.put(serializeKey(new Windowed<>(key, windows[0])), SerializeValue(50L));
            bytesStore.put(serializeKey(new Windowed<>(key, windows[3])), SerializeValue(100L));
            Assert.Equal(2, bytesStore.getSegments().Count);

            StateRestoreListener restoreListener = context.getRestoreListener(bytesStore.name());

            restoreListener.onRestoreStart(null, bytesStore.name(), 0L, 0L);

            foreach (S segment in bytesStore.getSegments())
            {
                Assert.Equal(GetOptions(segment).level0FileNumCompactionTrigger(), (1 << 30));
            }

            restoreListener.onRestoreEnd(null, bytesStore.name(), 0L);
            foreach (S segment in bytesStore.getSegments())
            {
                Assert.Equal(GetOptions(segment).level0FileNumCompactionTrigger(), (4));
            }
        }

        [Xunit.Fact]
        public void ShouldLogAndMeasureExpiredRecords()
        {
            LogCaptureAppender.setClassLoggerToDebug(AbstractRocksDBSegmentedBytesStore);
            LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

            // write a record to advance stream time, with a high enough timestamp
            // that the subsequent record in windows[0] will already be expired.
            bytesStore.put(serializeKey(new Windowed<>("dummy", nextSegmentWindow)), SerializeValue(0));

            Bytes key = serializeKey(new Windowed<>("a", windows[0]));
            byte[] value = SerializeValue(5);
            bytesStore.put(key, value);

            LogCaptureAppender.Unregister(appender);

            Dictionary < MetricName, ? : Metric > metrics = context.metrics().metrics();

            Metric dropTotal = metrics.get(new MetricName(
                "expired-window-record-drop-total",
                "stream-metrics-scope-metrics",
                "The total number of occurrence of expired-window-record-drop operations.",
                mkMap(
                    mkEntry("client-id", "mock"),
                    mkEntry("task-id", "0_0"),
                    mkEntry("metrics-scope-id", "bytes-store")
                )
            ));

            Metric dropRate = metrics.get(new MetricName(
                "expired-window-record-drop-rate",
                "stream-metrics-scope-metrics",
                "The average number of occurrence of expired-window-record-drop operation per second.",
                mkMap(
                    mkEntry("client-id", "mock"),
                    mkEntry("task-id", "0_0"),
                    mkEntry("metrics-scope-id", "bytes-store")
                )
            ));

            Assert.Equal(1.0, dropTotal.metricValue());
            Assert.NotEqual(0.0, dropRate.metricValue());
            List<string> messages = appender.getMessages();
            Assert.Equal(messages, hasItem("Skipping record for expired segment."));
        }

        private HashSet<string> SegmentDirs()
        {
            File windowDir = new File(stateDir, storeName);

            return Utils.mkSet(Objects.requireNonNull(windowDir.list()));
        }

        private Bytes SerializeKey(Windowed<string> key)
        {
            StateSerdes<string, long> stateSerdes = StateSerdes.withBuiltinTypes("dummy", string, long);
            if (schema is SessionKeySchema)
            {
                return Bytes.wrap(SessionKeySchema.toBinary(key, stateSerdes.keySerializer(), "dummy"));
            }
            else
            {
                return WindowKeySchema.toStoreKeyBinary(key, 0, stateSerdes);
            }
        }

        private byte[] SerializeValue(long value)
        {
            return Serdes.Long().Serializer.serialize("", value);
        }

        private List<KeyValuePair<Windowed<string>, long>> ToList(KeyValueIterator<Bytes, byte[]> iterator)
        {
            List<KeyValuePair<Windowed<string>, long>> results = new ArrayList<>();
            StateSerdes<string, long> stateSerdes = StateSerdes.withBuiltinTypes("dummy", string, long);
            while (iterator.hasNext())
            {
                KeyValuePair<Bytes, byte[]> next = iterator.next();
                if (schema is WindowKeySchema)
                {
                    KeyValuePair<Windowed<string>, long> deserialized = KeyValuePair.Create(
                        WindowKeySchema.fromStoreKey(next.key.get(), windowSizeForTimeWindow, stateSerdes.keyDeserializer(), stateSerdes.topic()),
                        stateSerdes.valueDeserializer().deserialize("dummy", next.value)
                    );
                    results.add(deserialized);
                }
                else
                {
                    KeyValuePair<Windowed<string>, long> deserialized = KeyValuePair.Create(
                        SessionKeySchema.from(next.key.get(), stateSerdes.keyDeserializer(), "dummy"),
                        stateSerdes.valueDeserializer().deserialize("dummy", next.value)
                    );
                    results.add(deserialized);
                }
            }
            return results;
        }
    }
}
/*






*

*





*/
























































