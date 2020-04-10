//using Confluent.Kafka;
//using Kafka.Streams.Configs;
//using Kafka.Streams.KStream;
//using Kafka.Streams.State.Internals;
//using Kafka.Streams.Tasks;

//namespace Kafka.Streams.Tests.State.Internals
//{
//    public class TimeOrderedKeyValueBufferTest<B>
//        where B : ITimeOrderedKeyValueBuffer<string, string>
//    {
//        private static Headers V_2_CHANGELOG_HEADERS =
//            new Headers(new Header[] { new Header("v", new byte[] { (byte)2 }) });

//        private const string APP_ID = "test-app";
//        private Function<string, B> bufferSupplier;
//        private readonly string testName;

//        // As we add more buffer implementations/configurations, we can add them here
//        // @Parameterized.Parameters(Name = "{index}: test={0}")
//        public static Collection<object[]> Parameters()
//        {
//            return singletonList(
//                new object[] {
//                "in-memory buffer",
//                (Function<string, InMemoryTimeOrderedKeyValueBuffer<string, string>>) Name =>
//                    new InMemoryTimeOrderedKeyValueBuffer
//                        .Builder<>(Name, Serdes.String(), Serdes.String())
//                        .Build()
//                }
//            );
//        }

//        public TimeOrderedKeyValueBufferTest(string testName, Function<string, B> bufferSupplier)
//        {
//            this.testName = testName + "_" + new Random().nextInt(int.MaxValue);
//            this.bufferSupplier = bufferSupplier;
//        }

//        private static MockInternalProcessorContext MakeContext()
//        {
//            StreamsConfig properties = new StreamsConfig();
//            properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
//            properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

//            TaskId taskId = new TaskId(0, 0);

//            MockInternalProcessorContext context = new MockInternalProcessorContext(properties, taskId, TestUtils.GetTempDirectory());
//            context.setRecordCollector(new MockRecordCollector());

//            return context;
//        }


//        private static void Cleanup(MockInternalProcessorContext context, ITimeOrderedKeyValueBuffer<string, string> buffer)
//        {
//            try
//            {
//                buffer.Close();
//                Utils.delete(context.stateDir());
//            }
//            catch (IOException e)
//            {
//                throw new RuntimeException(e);
//            }
//        }

//        [Fact]
//        public void ShouldInit()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);
//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldAcceptData()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);
//            PutRecord(buffer, context, 0L, 0L, "asdf", "2p93nf");
//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldRejectNullValues()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);
//            try
//            {
//                buffer.Put(0, "asdf", null, GetContext(0));
//                Assert.True(false, "expected an exception");
//            }
//            catch (NullPointerException expected)
//            {
//                // expected
//            }
//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldRemoveData()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);
//            PutRecord(buffer, context, 0L, 0L, "asdf", "qwer");
//            Assert.Equal(buffer.numRecords(), (1));
//            buffer.evictWhile(() => true, kv => { });
//            Assert.Equal(buffer.numRecords(), (0));
//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldRespectEvictionPredicate()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);
//            PutRecord(buffer, context, 0L, 0L, "asdf", "eyt");
//            PutRecord(buffer, context, 1L, 0L, "zxcv", "rtg");
//            Assert.Equal(buffer.numRecords(), (2));
//            List<Eviction<string, string>> evicted = new LinkedList<>();
//            buffer.evictWhile(() => buffer.numRecords() > 1, evicted::add);
//            Assert.Equal(buffer.numRecords(), (1));
//            Assert.Equal(evicted, (singletonList(
//                new Eviction<>("asdf", new Change<>("eyt", null), GetContext(0L))
//            )));
//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldTrackCount()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);
//            PutRecord(buffer, context, 0L, 0L, "asdf", "oin");
//            Assert.Equal(buffer.numRecords(), (1));
//            PutRecord(buffer, context, 1L, 0L, "asdf", "wekjn");
//            Assert.Equal(buffer.numRecords(), (1));
//            PutRecord(buffer, context, 0L, 0L, "zxcv", "24inf");
//            Assert.Equal(buffer.numRecords(), (2));
//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldTrackSize()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);
//            PutRecord(buffer, context, 0L, 0L, "asdf", "23roni");
//            Assert.Equal(buffer.bufferSize(), (43L));
//            PutRecord(buffer, context, 1L, 0L, "asdf", "3l");
//            Assert.Equal(buffer.bufferSize(), (39L));
//            PutRecord(buffer, context, 0L, 0L, "zxcv", "qfowin");
//            Assert.Equal(buffer.bufferSize(), (82L));
//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldTrackMinTimestamp()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);
//            PutRecord(buffer, context, 1L, 0L, "asdf", "2093j");
//            Assert.Equal(buffer.minTimestamp(), (1L));
//            PutRecord(buffer, context, 0L, 0L, "zxcv", "3gon4i");
//            Assert.Equal(buffer.minTimestamp(), (0L));
//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldEvictOldestAndUpdateSizeAndCountAndMinTimestamp()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);

//            PutRecord(buffer, context, 1L, 0L, "zxcv", "o23i4");
//            Assert.Equal(buffer.numRecords(), (1));
//            Assert.Equal(buffer.bufferSize(), (42L));
//            Assert.Equal(buffer.minTimestamp(), (1L));

//            PutRecord(buffer, context, 0L, 0L, "asdf", "3ng");
//            Assert.Equal(buffer.numRecords(), (2));
//            Assert.Equal(buffer.bufferSize(), (82L));
//            Assert.Equal(buffer.minTimestamp(), (0L));

//            AtomicInteger callbackCount = new AtomicInteger(0);
//            buffer.evictWhile(() => true, kv =>
//            {
//                switch (callbackCount.incrementAndGet())
//                {
//                    case 1:
//                        {
//                            Assert.Equal(kv.Key, ("asdf"));
//                            Assert.Equal(buffer.numRecords(), (2));
//                            Assert.Equal(buffer.bufferSize(), (82L));
//                            Assert.Equal(buffer.minTimestamp(), (0L));
//                            break;
//                        }
//                    case 2:
//                        {
//                            Assert.Equal(kv.Key, ("zxcv"));
//                            Assert.Equal(buffer.numRecords(), (1));
//                            Assert.Equal(buffer.bufferSize(), (42L));
//                            Assert.Equal(buffer.minTimestamp(), (1L));
//                            break;
//                        }
//                    default:
//                        {
//                            Assert.True(false, "too many invocations");
//                            break;
//                        }
//                }
//            });
//            Assert.Equal(callbackCount.Get(), (2));
//            Assert.Equal(buffer.numRecords(), (0));
//            Assert.Equal(buffer.bufferSize(), (0L));
//            Assert.Equal(buffer.minTimestamp(), (long.MaxValue));
//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldReturnUndefinedOnPriorValueForNotBufferedKey()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);

//            Assert.Equal(buffer.priorValueForBuffered("ASDF"), (Maybe.undefined()));
//        }

//        [Fact]
//        public void ShouldReturnPriorValueForBufferedKey()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);

//            ProcessorRecordContext recordContext = GetContext(0L);
//            context.setRecordContext(recordContext);
//            buffer.Put(1L, "A", new Change<>("new-value", "old-value"), recordContext);
//            buffer.Put(1L, "B", new Change<>("new-value", null), recordContext);
//            Assert.Equal(buffer.priorValueForBuffered("A"), (Maybe.defined(ValueAndTimestamp.Make("old-value", -1))));
//            Assert.Equal(buffer.priorValueForBuffered("B"), (Maybe.defined(null)));
//        }

//        [Fact]
//        public void ShouldFlush()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);
//            PutRecord(buffer, context, 2L, 0L, "asdf", "2093j");
//            PutRecord(buffer, context, 1L, 1L, "zxcv", "3gon4i");
//            PutRecord(buffer, context, 0L, 2L, "deleteme", "deadbeef");

//            // replace "deleteme" with a tombstone
//            buffer.evictWhile(() => buffer.minTimestamp() < 1, kv => { });

//            // Flush everything to the changelog
//            buffer.Flush();

//            // the buffer should serialize the buffer time and the value as byte[],
//            // which we can't compare for equality using ProducerRecord.
//            // As a workaround, I'm deserializing them and shoving them in a KeyValuePair, just for ease of testing.

//            List<ProducerRecord<string, KeyValuePair<long, BufferValue>>> collected =
//                ((MockRecordCollector)context.recordCollector())
//                    .collected()
//                    .Stream()
//                    .map(pr =>
//                    {
//                        KeyValuePair<long, BufferValue> niceValue;
//                        if (pr.Value == null)
//                        {
//                            niceValue = null;
//                        }
//                        else
//                        {
//                            byte[] serializedValue = pr.Value;
//                            ByteBuffer valueBuffer = new ByteBuffer().Wrap(serializedValue);
//                            BufferValue contextualRecord = BufferValue.deserialize(valueBuffer);
//                            long timestamp = valueBuffer.GetLong();
//                            niceValue = KeyValuePair.Create(timestamp, contextualRecord);
//                        }

//                        return new ProducerRecord<>(pr.Topic,
//                                                    pr.Partition,
//                                                    pr.Timestamp,
//                                                    new string(pr.Key, UTF_8),
//                                                    niceValue,
//                                                    pr.Headers);
//                    })
//                    .collect(Collectors.toList());

//            Assert.Equal(collected, (asList(
//                new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
//                                     0,   // Producer will assign
//                                     null,
//                                     "deleteme",
//                                     null,
//                                     new Headers()
//                ),
//                new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
//                                     0,
//                                     null,
//                                     "zxcv",
//                                     KeyValuePair.Create(1L, GetBufferValue("3gon4i", 1)),
//                                     V_2_CHANGELOG_HEADERS
//                ),
//                new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
//                                     0,
//                                     null,
//                                     "asdf",
//                                     KeyValuePair.Create(2L, GetBufferValue("2093j", 0)),
//                                     V_2_CHANGELOG_HEADERS
//                )
//            )));

//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldRestoreOldFormat()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);

//            RecordBatchingStateRestoreCallback stateRestoreCallback =
//                (RecordBatchingStateRestoreCallback)context.stateRestoreCallback(testName);

//            context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

//            FullChangeSerde<string> serializer = FullChangeSerde.Wrap(Serdes.String());

//            byte[] todeleteValue = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serializer.serializeParts(null, new Change<>("doomed", null)));
//            byte[] asdfValue = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serializer.serializeParts(null, new Change<>("qwer", null)));
//            byte[] zxcvValue1 = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serializer.serializeParts(null, new Change<>("eo4im", "previous")));
//            byte[] zxcvValue2 = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serializer.serializeParts(null, new Change<>("next", "eo4im")));
//            stateRestoreCallback.restoreBatch(asList(
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     0,
//                                     0,
//                                     TimestampType.CreateTime,
//                                     -1,
//                                     -1,
//                                     -1,
//                                     "todelete".getBytes(UTF_8),
//                                     new ByteBuffer().Allocate(long.BYTES + todeleteValue.Length).putLong(0L).Put(todeleteValue).array()),
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     1,
//                                     1,
//                                     TimestampType.CreateTime,
//                                     -1,
//                                     -1,
//                                     -1,
//                                     "asdf".getBytes(UTF_8),
//                                     new ByteBuffer().Allocate(long.BYTES + asdfValue.Length).putLong(2L).Put(asdfValue).array()),
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     2,
//                                     2,
//                                     TimestampType.CreateTime,
//                                     -1,
//                                     -1,
//                                     -1,
//                                     "zxcv".getBytes(UTF_8),
//                                     new ByteBuffer().Allocate(long.BYTES + zxcvValue1.Length).putLong(1L).Put(zxcvValue1).array()),
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     3,
//                                     3,
//                                     TimestampType.CreateTime,
//                                     -1,
//                                     -1,
//                                     -1,
//                                     "zxcv".getBytes(UTF_8),
//                                     new ByteBuffer().Allocate(long.BYTES + zxcvValue2.Length).putLong(1L).Put(zxcvValue2).array())
//            ));

//            Assert.Equal(buffer.numRecords(), (3));
//            Assert.Equal(buffer.minTimestamp(), (0L));
//            Assert.Equal(buffer.bufferSize(), (172L));

//            stateRestoreCallback.restoreBatch(singletonList(
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     3,
//                                     3,
//                                     TimestampType.CreateTime,
//                                     -1,
//                                     -1,
//                                     -1,
//                                     "todelete".getBytes(UTF_8),
//                                     null)
//            ));

//            Assert.Equal(buffer.numRecords(), (2));
//            Assert.Equal(buffer.minTimestamp(), (1L));
//            Assert.Equal(buffer.bufferSize(), (115L));

//            Assert.Equal(buffer.priorValueForBuffered("todelete"), (Maybe.undefined()));
//            Assert.Equal(buffer.priorValueForBuffered("asdf"), (Maybe.defined(null)));
//            Assert.Equal(buffer.priorValueForBuffered("zxcv"), (Maybe.defined(ValueAndTimestamp.Make("previous", -1))));

//            // Flush the buffer into a list in buffer order so we can make assertions about the contents.

//            List<Eviction<string, string>> evicted = new LinkedList<>();
//            buffer.evictWhile(() => true, evicted::add);

//            // Several things to note:
//            // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
//            // * The record timestamps are properly restored, and not conflated with the record's buffer time.
//            // * The keys and values are properly restored
//            // * The record topic is set to the changelog topic. This was an oversight in the original implementation,
//            //   which is fixed in changelog format v1. But upgraded applications still need to be able to handle the
//            //   original format.

//            Assert.Equal(evicted, (asList(
//                new Eviction<>(
//                    "zxcv",
//                    new Change<>("next", "eo4im"),
//                    new ProcessorRecordContext(3L, 3, 0, "changelog-topic", new Headers())),
//                new Eviction<>(
//                    "asdf",
//                    new Change<>("qwer", null),
//                    new ProcessorRecordContext(1L, 1, 0, "changelog-topic", new Headers()))
//            )));

//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldRestoreV1Format()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);

//            RecordBatchingStateRestoreCallback stateRestoreCallback =
//                (RecordBatchingStateRestoreCallback)context.stateRestoreCallback(testName);

//            context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

//            Headers v1FlagHeaders = new Headers(new Header[] { new RecordHeader("v", new byte[] { (byte)1 }) });

//            byte[] todeleteValue = GetContextualRecord("doomed", 0).Serialize(0).array();
//            byte[] asdfValue = GetContextualRecord("qwer", 1).Serialize(0).array();
//            FullChangeSerde<string> fullChangeSerde = FullChangeSerde.Wrap(Serdes.String());
//            byte[] zxcvValue1 = new ContextualRecord(
//                FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(fullChangeSerde.serializeParts(null, new Change<>("3o4im", "previous"))),
//                GetContext(2L)
//            ).Serialize(0).array();
//            byte[] zxcvValue2 = new ContextualRecord(
//                FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(fullChangeSerde.serializeParts(null, new Change<>("next", "3o4im"))),
//                GetContext(3L)
//            ).Serialize(0).array();
//            stateRestoreCallback.restoreBatch(asList(
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     0,
//                                     999,
//                                     TimestampType.CreateTime,
//                                     -1L,
//                                     -1,
//                                     -1,
//                                     "todelete".getBytes(UTF_8),
//                                     new ByteBuffer().Allocate(long.BYTES + todeleteValue.Length).putLong(0L).Put(todeleteValue).array(),
//                                     v1FlagHeaders),
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     1,
//                                     9999,
//                                     TimestampType.CreateTime,
//                                     -1L,
//                                     -1,
//                                     -1,
//                                     "asdf".getBytes(UTF_8),
//                                     new ByteBuffer().Allocate(long.BYTES + asdfValue.Length).putLong(2L).Put(asdfValue).array(),
//                                     v1FlagHeaders),
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     2,
//                                     99,
//                                     TimestampType.CreateTime,
//                                     -1L,
//                                     -1,
//                                     -1,
//                                     "zxcv".getBytes(UTF_8),
//                                     new ByteBuffer().Allocate(long.BYTES + zxcvValue1.Length).putLong(1L).Put(zxcvValue1).array(),
//                                     v1FlagHeaders),
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     3,
//                                     100,
//                                     TimestampType.CreateTime,
//                                     -1L,
//                                     -1,
//                                     -1,
//                                     "zxcv".getBytes(UTF_8),
//                                     new ByteBuffer().Allocate(long.BYTES + zxcvValue2.Length).putLong(1L).Put(zxcvValue2).array(),
//                                     v1FlagHeaders)
//            ));

//            Assert.Equal(buffer.numRecords(), (3));
//            Assert.Equal(buffer.minTimestamp(), (0L));
//            Assert.Equal(buffer.bufferSize(), (142L));

//            stateRestoreCallback.restoreBatch(singletonList(
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     3,
//                                     3,
//                                     TimestampType.CreateTime,
//                                     -1L,
//                                     -1,
//                                     -1,
//                                     "todelete".getBytes(UTF_8),
//                                     null)
//            ));

//            Assert.Equal(buffer.numRecords(), (2));
//            Assert.Equal(buffer.minTimestamp(), (1L));
//            Assert.Equal(buffer.bufferSize(), (95L));

//            Assert.Equal(buffer.priorValueForBuffered("todelete"), (Maybe.undefined()));
//            Assert.Equal(buffer.priorValueForBuffered("asdf"), (Maybe.defined(null)));
//            Assert.Equal(buffer.priorValueForBuffered("zxcv"), (Maybe.defined(ValueAndTimestamp.Make("previous", -1))));

//            // Flush the buffer into a list in buffer order so we can make assertions about the contents.

//            List<Eviction<string, string>> evicted = new LinkedList<>();
//            buffer.evictWhile(() => true, evicted::add);

//            // Several things to note:
//            // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
//            // * The record timestamps are properly restored, and not conflated with the record's buffer time.
//            // * The keys and values are properly restored
//            // * The record topic is set to the original input topic, *not* the changelog topic
//            // * The record offset preserves the original input record's offset, *not* the offset of the changelog record


//            Assert.Equal(evicted, (asList(
//                new Eviction<>(
//                    "zxcv",
//                    new Change<>("next", "3o4im"),
//                    GetContext(3L)),
//                new Eviction<>(
//                    "asdf",
//                    new Change<>("qwer", null),
//                    GetContext(1L)
//                ))));

//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldRestoreV2Format()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);

//            RecordBatchingStateRestoreCallback stateRestoreCallback =
//                (RecordBatchingStateRestoreCallback)context.stateRestoreCallback(testName);

//            context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

//            Headers v2FlagHeaders = new Headers(new Header[] { new RecordHeader("v", new byte[] { (byte)2 }) });

//            byte[] todeleteValue = GetBufferValue("doomed", 0).Serialize(0).array();
//            byte[] asdfValue = GetBufferValue("qwer", 1).Serialize(0).array();
//            FullChangeSerde<string> fullChangeSerde = FullChangeSerde.Wrap(Serdes.String());
//            byte[] zxcvValue1 =
//                new BufferValue(
//                    Serdes.String().Serializer.Serialize(null, "previous"),
//                    Serdes.String().Serializer.Serialize(null, "IGNORED"),
//                    Serdes.String().Serializer.Serialize(null, "3o4im"),
//                    GetContext(2L)
//                ).Serialize(0).array();
//            FullChangeSerde<string> fullChangeSerde1 = FullChangeSerde.Wrap(Serdes.String());
//            byte[] zxcvValue2 =
//                new BufferValue(
//                    Serdes.String().Serializer.Serialize(null, "previous"),
//                    Serdes.String().Serializer.Serialize(null, "3o4im"),
//                    Serdes.String().Serializer.Serialize(null, "next"),
//                    GetContext(3L)
//                ).Serialize(0).array();
//            stateRestoreCallback.restoreBatch(asList(
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     0,
//                                     999,
//                                     TimestampType.CreateTime,
//                                     -1L,
//                                     -1,
//                                     -1,
//                                     "todelete".getBytes(UTF_8),
//                                     new ByteBuffer().Allocate(long.BYTES + todeleteValue.Length).Put(todeleteValue).putLong(0L).array(),
//                                     v2FlagHeaders),
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     1,
//                                     9999,
//                                     TimestampType.CreateTime,
//                                     -1L,
//                                     -1,
//                                     -1,
//                                     "asdf".getBytes(UTF_8),
//                                     new ByteBuffer().Allocate(long.BYTES + asdfValue.Length).Put(asdfValue).putLong(2L).array(),
//                                     v2FlagHeaders),
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     2,
//                                     99,
//                                     TimestampType.CreateTime,
//                                     -1L,
//                                     -1,
//                                     -1,
//                                     "zxcv".getBytes(UTF_8),
//                                     new ByteBuffer().Allocate(long.BYTES + zxcvValue1.Length).Put(zxcvValue1).putLong(1L).array(),
//                                     v2FlagHeaders),
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     2,
//                                     100,
//                                     TimestampType.CreateTime,
//                                     -1L,
//                                     -1,
//                                     -1,
//                                     "zxcv".getBytes(UTF_8),
//                                     new ByteBuffer().Allocate(long.BYTES + zxcvValue2.Length).Put(zxcvValue2).putLong(1L).array(),
//                                     v2FlagHeaders)
//            ));

//            Assert.Equal(buffer.numRecords(), (3));
//            Assert.Equal(buffer.minTimestamp(), (0L));
//            Assert.Equal(buffer.bufferSize(), (142L));

//            stateRestoreCallback.restoreBatch(singletonList(
//                new ConsumeResult<>("changelog-topic",
//                                     0,
//                                     3,
//                                     3,
//                                     TimestampType.CreateTime,
//                                     -1L,
//                                     -1,
//                                     -1,
//                                     "todelete".getBytes(UTF_8),
//                                     null)
//            ));

//            Assert.Equal(buffer.numRecords(), (2));
//            Assert.Equal(buffer.minTimestamp(), (1L));
//            Assert.Equal(buffer.bufferSize(), (95L));

//            Assert.Equal(buffer.priorValueForBuffered("todelete"), (Maybe.undefined()));
//            Assert.Equal(buffer.priorValueForBuffered("asdf"), (Maybe.defined(null)));
//            Assert.Equal(buffer.priorValueForBuffered("zxcv"), (Maybe.defined(ValueAndTimestamp.Make("previous", -1))));

//            // Flush the buffer into a list in buffer order so we can make assertions about the contents.

//            List<Eviction<string, string>> evicted = new LinkedList<>();
//            buffer.evictWhile(() => true, evicted::add);

//            // Several things to note:
//            // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
//            // * The record timestamps are properly restored, and not conflated with the record's buffer time.
//            // * The keys and values are properly restored
//            // * The record topic is set to the original input topic, *not* the changelog topic
//            // * The record offset preserves the original input record's offset, *not* the offset of the changelog record


//            Assert.Equal(evicted, (asList(
//                new Eviction<>(
//                    "zxcv",
//                    new Change<>("next", "3o4im"),
//                    GetContext(3L)),
//                new Eviction<>(
//                    "asdf",
//                    new Change<>("qwer", null),
//                    GetContext(1L)
//                ))));

//            Cleanup(context, buffer);
//        }

//        [Fact]
//        public void ShouldNotRestoreUnrecognizedVersionRecord()
//        {
//            ITimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
//            MockInternalProcessorContext context = MakeContext();
//            buffer.Init(context, buffer);

//            RecordBatchingStateRestoreCallback stateRestoreCallback =
//                (RecordBatchingStateRestoreCallback)context.stateRestoreCallback(testName);

//            context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

//            Headers unknownFlagHeaders = new Headers(new Header[] { new RecordHeader("v", new byte[] { (byte)-1 }) });

//            byte[] todeleteValue = GetBufferValue("doomed", 0).Serialize(0).array();
//            try
//            {
//                stateRestoreCallback.restoreBatch(singletonList(
//                    new ConsumeResult<>("changelog-topic",
//                                         0,
//                                         0,
//                                         999,
//                                         TimestampType.CreateTime,
//                                         -1L,
//                                         -1,
//                                         -1,
//                                         "todelete".getBytes(UTF_8),
//                                         new ByteBuffer().Allocate(long.BYTES + todeleteValue.Length).putLong(0L).Put(todeleteValue).array(),
//                                         unknownFlagHeaders)
//                ));
//                Assert.True(false, "expected an exception");
//            }
//            catch (ArgumentException expected)
//            {
//                // nothing to do.
//            }
//            finally
//            {
//                Cleanup(context, buffer);
//            }
//        }

//        private static void PutRecord(ITimeOrderedKeyValueBuffer<string, string> buffer,
//                                      MockInternalProcessorContext context,
//                                      long streamTime,
//                                      long recordTimestamp,
//                                      string key,
//                                      string value)
//        {
//            ProcessorRecordContext recordContext = GetContext(recordTimestamp);
//            context.setRecordContext(recordContext);
//            buffer.Put(streamTime, key, new Change<>(value, null), recordContext);
//        }

//        private static BufferValue GetBufferValue(string value, long timestamp)
//        {
//            return new BufferValue(
//                null,
//                null,
//                Serdes.String().Serializer.Serialize(null, value),
//                GetContext(timestamp)
//            );
//        }

//        private static ContextualRecord GetContextualRecord(string value, long timestamp)
//        {
//            FullChangeSerde<string> fullChangeSerde = FullChangeSerde.Wrap(Serdes.String());
//            return new ContextualRecord(
//                FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(fullChangeSerde.serializeParts(null, new Change<>(value, null))),
//                GetContext(timestamp)
//            );
//        }

//        private static ProcessorRecordContext GetContext(long recordTimestamp)
//        {
//            return new ProcessorRecordContext(recordTimestamp, 0, 0, "topic", null);
//        }
//    }
//}
