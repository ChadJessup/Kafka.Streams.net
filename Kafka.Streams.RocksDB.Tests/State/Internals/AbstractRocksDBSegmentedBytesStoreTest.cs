//using Kafka.Streams.KStream;
//using System.IO;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public abstract class AbstractRocksDBSegmentedBytesStoreTest<Segment>
//    {
//        private readonly long windowSizeForTimeWindow = 500;
//        private InternalMockProcessorContext context;
//        private AbstractRocksDBSegmentedBytesStore<S> bytesStore;
//        private DirectoryInfo stateDir;
//        private readonly Window[] windows = new Window[4];
//        private Window nextSegmentWindow;
//        readonly long retention = 1000;
//        readonly long segmentInterval = 60_000L;
//        readonly string storeName = "bytes-store";


//        public SegmentedBytesStore.KeySchema schema;

//        // @Parameters(Name = "{0}")
//        public static object[] GetKeySchemas()
//        {
//            return new object[] { new SessionKeySchema(), new WindowKeySchema() };
//        }


//        public void Before()
//        {
//            if (schema is SessionKeySchema)
//            {
//                windows[0] = new SessionWindow(10L, 10L);
//                windows[1] = new SessionWindow(500L, 1000L);
//                windows[2] = new SessionWindow(1_000L, 1_500L);
//                windows[3] = new SessionWindow(30_000L, 60_000L);
//                // All four of the previous windows will go into segment 1.
//                // The nextSegmentWindow is computed be a high enough time that when it gets written
//                // to the segment store, it will advance stream time past the first segment's retention time and
//                // expire it.
//                nextSegmentWindow = new SessionWindow(segmentInterval + retention, segmentInterval + retention);
//            }
//            if (schema is WindowKeySchema)
//            {
//                windows[0] = timeWindowForSize(10L, windowSizeForTimeWindow);
//                windows[1] = timeWindowForSize(500L, windowSizeForTimeWindow);
//                windows[2] = timeWindowForSize(1_000L, windowSizeForTimeWindow);
//                windows[3] = timeWindowForSize(60_000L, windowSizeForTimeWindow);
//                // All four of the previous windows will go into segment 1.
//                // The nextSegmentWindow is computed be a high enough time that when it gets written
//                // to the segment store, it will advance stream time past the first segment's retention time and
//                // expire it.
//                nextSegmentWindow = timeWindowForSize(segmentInterval + retention, windowSizeForTimeWindow);
//            }

//            bytesStore = GetBytesStore();

//            stateDir = TestUtils.GetTempDirectory();
//            context = new InternalMockProcessorContext(
//                stateDir,
//                Serdes.String(),
//                Serdes.Long(),
//                new NoOpRecordCollector(),
//                new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
//            );
//            bytesStore.Init(context, bytesStore);
//        }


//        public void Close()
//        {
//            bytesStore.Close();
//        }

//        abstract AbstractRocksDBSegmentedBytesStore<S> GetBytesStore();

//        abstract AbstractSegments<S> NewSegments();

//        abstract Options GetOptions(S segment);

//        [Fact]
//        public void ShouldPutAndFetch()
//        {
//            string key = "a";
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[0])), SerializeValue(10));
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[1])), SerializeValue(50));
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[2])), SerializeValue(100));

//            IKeyValueIterator<Bytes, byte[]> values = bytesStore.Fetch(Bytes.Wrap(key.getBytes()), 0, 500);

//            List<KeyValuePair<IWindowed<string>, long>> expected = Arrays.asList(
//                KeyValuePair.Create(new IWindowed<>(key, windows[0]), 10L),
//                KeyValuePair.Create(new IWindowed<>(key, windows[1]), 50L)
//            );

//            Assert.Equal(expected, ToList(values));
//        }

//        [Fact]
//        public void ShouldFindValuesWithinRange()
//        {
//            string key = "a";
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[0])), SerializeValue(10));
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[1])), SerializeValue(50));
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[2])), SerializeValue(100));
//            IKeyValueIterator<Bytes, byte[]> results = bytesStore.Fetch(Bytes.Wrap(key.getBytes()), 1, 999);
//            List<KeyValuePair<IWindowed<string>, long>> expected = Arrays.asList(
//                KeyValuePair.Create(new IWindowed<>(key, windows[0]), 10L),
//                KeyValuePair.Create(new IWindowed<>(key, windows[1]), 50L)
//            );

//            Assert.Equal(expected, ToList(results));
//        }

//        [Fact]
//        public void ShouldRemove()
//        {
//            bytesStore.Put(serializeKey(new IWindowed<>("a", windows[0])), SerializeValue(30));
//            bytesStore.Put(serializeKey(new IWindowed<>("a", windows[1])), SerializeValue(50));

//            bytesStore.remove(serializeKey(new IWindowed<>("a", windows[0])));
//            IKeyValueIterator<Bytes, byte[]> value = bytesStore.Fetch(Bytes.Wrap("a".getBytes()), 0, 100);
//            Assert.False(value.MoveNext());
//        }

//        [Fact]
//        public void ShouldRollSegments()
//        {
//            // just to validate directories
//            AbstractSegments<S> segments = NewSegments();
//            string key = "a";

//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[0])), SerializeValue(50));
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[1])), SerializeValue(100));
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[2])), SerializeValue(500));
//            Assert.Equal(Collections.singleton(segments.segmentName(0)), SegmentDirs());

//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[3])), SerializeValue(1000));
//            Assert.Equal(Utils.mkSet(segments.segmentName(0), segments.segmentName(1)), SegmentDirs());

//            List<KeyValuePair<IWindowed<string>, long>> results = toList(bytesStore.Fetch(Bytes.Wrap(key.getBytes()), 0, 1500));

//            Assert.Equal(
//                Arrays.asList(
//                    KeyValuePair.Create(new IWindowed<>(key, windows[0]), 50L),
//                    KeyValuePair.Create(new IWindowed<>(key, windows[1]), 100L),
//                    KeyValuePair.Create(new IWindowed<>(key, windows[2]), 500L)
//                ),
//                results
//            );
//        }

//        [Fact]
//        public void ShouldGetAllSegments()
//        {
//            // just to validate directories
//            AbstractSegments<S> segments = NewSegments();
//            string key = "a";

//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[0])), SerializeValue(50L));
//            Assert.Equal(Collections.singleton(segments.segmentName(0)), SegmentDirs());

//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[3])), SerializeValue(100L));
//            Assert.Equal(
//                Utils.mkSet(
//                    segments.segmentName(0),
//                    segments.segmentName(1)
//                ),
//                SegmentDirs()
//            );

//            List<KeyValuePair<IWindowed<string>, long>> results = toList(bytesStore.All());
//            Assert.Equal(
//                Arrays.asList(
//                    KeyValuePair.Create(new IWindowed<>(key, windows[0]), 50L),
//                    KeyValuePair.Create(new IWindowed<>(key, windows[3]), 100L)
//                ),
//                results
//            );
//        }

//        [Fact]
//        public void ShouldFetchAllSegments()
//        {
//            // just to validate directories
//            AbstractSegments<S> segments = NewSegments();
//            string key = "a";

//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[0])), SerializeValue(50L));
//            Assert.Equal(Collections.singleton(segments.segmentName(0)), SegmentDirs());

//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[3])), SerializeValue(100L));
//            Assert.Equal(
//                Utils.mkSet(
//                    segments.segmentName(0),
//                    segments.segmentName(1)
//                ),
//                SegmentDirs()
//            );

//            List<KeyValuePair<IWindowed<string>, long>> results = toList(bytesStore.FetchAll(0L, 60_000L));
//            Assert.Equal(
//                Arrays.asList(
//                    KeyValuePair.Create(new IWindowed<>(key, windows[0]), 50L),
//                    KeyValuePair.Create(new IWindowed<>(key, windows[3]), 100L)
//                ),
//                results
//            );
//        }

//        [Fact]
//        public void ShouldLoadSegmentsWithOldStyleDateFormattedName()
//        {
//            AbstractSegments<S> segments = NewSegments();
//            string key = "a";

//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[0])), SerializeValue(50L));
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[3])), SerializeValue(100L));
//            bytesStore.Close();

//            string firstSegmentName = segments.segmentName(0);
//            string[] nameParts = firstSegmentName.Split("\\.");
//            long segmentId = long.parseLong(nameParts[1]);
//            SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
//            formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
//            string formatted = formatter.format(new Date(segmentId * segmentInterval));
//            File parent = new File(stateDir, storeName);
//            File oldStyleName = new File(parent, nameParts[0] + "-" + formatted);
//            Assert.True(new File(parent, firstSegmentName).renameTo(oldStyleName));

//            bytesStore = GetBytesStore();

//            bytesStore.Init(context, bytesStore);
//            List<KeyValuePair<IWindowed<string>, long>> results = toList(bytesStore.Fetch(Bytes.Wrap(key.getBytes()), 0L, 60_000L));
//            Assert.Equal(
//                results,
//                equalTo(
//                    Arrays.asList(
//                        KeyValuePair.Create(new IWindowed<>(key, windows[0]), 50L),
//                        KeyValuePair.Create(new IWindowed<>(key, windows[3]), 100L)
//                    )
//                )
//            );
//        }

//        [Fact]
//        public void ShouldLoadSegmentsWithOldStyleColonFormattedName()
//        {
//            AbstractSegments<S> segments = NewSegments();
//            string key = "a";

//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[0])), SerializeValue(50L));
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[3])), SerializeValue(100L));
//            bytesStore.Close();

//            string firstSegmentName = segments.segmentName(0);
//            string[] nameParts = firstSegmentName.Split("\\.");
//            File parent = new File(stateDir, storeName);
//            File oldStyleName = new File(parent, nameParts[0] + ":" + long.parseLong(nameParts[1]));
//            Assert.True(new File(parent, firstSegmentName).renameTo(oldStyleName));

//            bytesStore = GetBytesStore();

//            bytesStore.Init(context, bytesStore);
//            List<KeyValuePair<IWindowed<string>, long>> results = toList(bytesStore.Fetch(Bytes.Wrap(key.getBytes()), 0L, 60_000L));
//            Assert.Equal(
//                results,
//                equalTo(
//                    Arrays.asList(
//                        KeyValuePair.Create(new IWindowed<>(key, windows[0]), 50L),
//                        KeyValuePair.Create(new IWindowed<>(key, windows[3]), 100L)
//                    )
//                )
//            );
//        }

//        [Fact]
//        public void ShouldBeAbleToWriteToReInitializedStore()
//        {
//            string key = "a";
//            // need to Create a segment so we can attempt to write to it again.
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[0])), SerializeValue(50));
//            bytesStore.Close();
//            bytesStore.Init(context, bytesStore);
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[1])), SerializeValue(100));
//        }

//        [Fact]
//        public void ShouldCreateWriteBatches()
//        {
//            string key = "a";
//            Collection<KeyValuePair<byte[], byte[]>> records = new List<>();
//            records.Add(KeyValuePair.Create(serializeKey(new IWindowed<>(key, windows[0])).Get(), SerializeValue(50L)));
//            records.Add(KeyValuePair.Create(serializeKey(new IWindowed<>(key, windows[3])).Get(), SerializeValue(100L)));
//            Dictionary<S, WriteBatch> writeBatchMap = bytesStore.getWriteBatches(records);
//            Assert.Equal(2, writeBatchMap.Count);
//            foreach (WriteBatch batch in writeBatchMap.values())
//            {
//                Assert.Equal(1, batch.count());
//            }
//        }

//        [Fact]
//        public void ShouldRestoreToByteStore()
//        {
//            // 0 segments initially.
//            Assert.Equal(0, bytesStore.getSegments().Count);
//            string key = "a";
//            Collection<KeyValuePair<byte[], byte[]>> records = new List<>();
//            records.Add(KeyValuePair.Create(serializeKey(new IWindowed<>(key, windows[0])).Get(), SerializeValue(50L)));
//            records.Add(KeyValuePair.Create(serializeKey(new IWindowed<>(key, windows[3])).Get(), SerializeValue(100L)));
//            bytesStore.restoreAllInternal(records);

//            // 2 segments are created during restoration.
//            Assert.Equal(2, bytesStore.getSegments().Count);

//            // Bulk loading is enabled during recovery.
//            foreach (S segment in bytesStore.getSegments())
//            {
//                Assert.Equal(GetOptions(segment).level0FileNumCompactionTrigger(), (1 << 30));
//            }

//            List<KeyValuePair<IWindowed<string>, long>> expected = new List<KeyValuePair<IWindowed<string>, long>>();
//            expected.Add(KeyValuePair.Create(new IWindowed<>(key, windows[0]), 50L));
//            expected.Add(KeyValuePair.Create(new IWindowed<>(key, windows[3]), 100L));

//            List<KeyValuePair<IWindowed<string>, long>> results = toList(bytesStore.All());
//            Assert.Equal(expected, results);
//        }

//        [Fact]
//        public void ShouldRespectBulkLoadOptionsDuringInit()
//        {
//            bytesStore.Init(context, bytesStore);
//            string key = "a";
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[0])), SerializeValue(50L));
//            bytesStore.Put(serializeKey(new IWindowed<>(key, windows[3])), SerializeValue(100L));
//            Assert.Equal(2, bytesStore.getSegments().Count);

//            StateRestoreListener restoreListener = context.getRestoreListener(bytesStore.Name());

//            restoreListener.onRestoreStart(null, bytesStore.Name(), 0L, 0L);

//            foreach (S segment in bytesStore.getSegments())
//            {
//                Assert.Equal(GetOptions(segment).level0FileNumCompactionTrigger(), (1 << 30));
//            }

//            restoreListener.onRestoreEnd(null, bytesStore.Name(), 0L);
//            foreach (S segment in bytesStore.getSegments())
//            {
//                Assert.Equal(GetOptions(segment).level0FileNumCompactionTrigger(), (4));
//            }
//        }

//        [Fact]
//        public void ShouldLogAndMeasureExpiredRecords()
//        {
//            LogCaptureAppender.setClassLoggerToDebug(AbstractRocksDBSegmentedBytesStore);
//            LogCaptureAppender appender = LogCaptureAppender.CreateAndRegister();

//            // write a record to advance stream time, with a high enough timestamp
//            // that the subsequent record in windows[0] will already be expired.
//            bytesStore.Put(serializeKey(new IWindowed<>("dummy", nextSegmentWindow)), SerializeValue(0));

//            Bytes key = serializeKey(new IWindowed<>("a", windows[0]));
//            byte[] value = SerializeValue(5);
//            bytesStore.Put(key, value);

//            LogCaptureAppender.Unregister(appender);

//            Dictionary < MetricName, ? : Metric > metrics = context.metrics().metrics();

//            Metric dropTotal = metrics.Get(new MetricName(
//                "expired-window-record-drop-total",
//                "stream-metrics-scope-metrics",
//                "The total number of occurrence of expired-window-record-drop operations.",
//                mkMap(
//                    mkEntry("client-id", "mock"),
//                    mkEntry("task-id", "0_0"),
//                    mkEntry("metrics-scope-id", "bytes-store")
//                )
//            ));

//            Metric dropRate = metrics.Get(new MetricName(
//                "expired-window-record-drop-rate",
//                "stream-metrics-scope-metrics",
//                "The average number of occurrence of expired-window-record-drop operation per second.",
//                mkMap(
//                    mkEntry("client-id", "mock"),
//                    mkEntry("task-id", "0_0"),
//                    mkEntry("metrics-scope-id", "bytes-store")
//                )
//            ));

//            Assert.Equal(1.0, dropTotal.metricValue());
//            Assert.NotEqual(0.0, dropRate.metricValue());
//            List<string> messages = appender.getMessages();
//            Assert.Equal(messages, hasItem("Skipping record for expired segment."));
//        }

//        private HashSet<string> SegmentDirs()
//        {
//            File windowDir = new File(stateDir, storeName);

//            return Utils.mkSet(Objects.requireNonNull(windowDir.list()));
//        }

//        private Bytes SerializeKey(IWindowed<string> key)
//        {
//            StateSerdes<string, long> stateSerdes = StateSerdes.WithBuiltinTypes("dummy", string, long);
//            if (schema is SessionKeySchema)
//            {
//                return Bytes.Wrap(SessionKeySchema.toBinary(key, stateSerdes.keySerializer(), "dummy"));
//            }
//            else
//            {
//                return WindowKeySchema.toStoreKeyBinary(key, 0, stateSerdes);
//            }
//        }

//        private byte[] SerializeValue(long value)
//        {
//            return Serdes.Long().Serializer.Serialize("", value);
//        }

//        private List<KeyValuePair<IWindowed<string>, long>> ToList(IKeyValueIterator<Bytes, byte[]> iterator)
//        {
//            List<KeyValuePair<IWindowed<string>, long>> results = new List<KeyValuePair<IWindowed<string>, long>>();
//            StateSerdes<string, long> stateSerdes = StateSerdes.WithBuiltinTypes("dummy", string, long);
//            while (iterator.MoveNext())
//            {
//                KeyValuePair<Bytes, byte[]> next = iterator.MoveNext();
//                if (schema is WindowKeySchema)
//                {
//                    KeyValuePair<IWindowed<string>, long> deserialized = KeyValuePair.Create(
//                        WindowKeySchema.fromStoreKey(next.key.Get(), windowSizeForTimeWindow, stateSerdes.keyDeserializer(), stateSerdes.Topic),
//                        stateSerdes.valueDeserializer().deserialize("dummy", next.value)
//                    );
//                    results.Add(deserialized);
//                }
//                else
//                {
//                    KeyValuePair<IWindowed<string>, long> deserialized = KeyValuePair.Create(
//                        SessionKeySchema.from(next.key.Get(), stateSerdes.keyDeserializer(), "dummy"),
//                        stateSerdes.valueDeserializer().deserialize("dummy", next.value)
//                    );
//                    results.Add(deserialized);
//                }
//            }
//            return results;
//        }
//    }
//}
