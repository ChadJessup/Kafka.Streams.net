/*






 *

 *





 */













































public class TimeOrderedKeyValueBufferTest<B : TimeOrderedKeyValueBuffer<string, string>> {
    private static Headers V_2_CHANGELOG_HEADERS =
        new Headers(new Header[] {new RecordHeader("v", new byte[] {(byte) 2})});

    private static readonly string APP_ID = "test-app";
    private Function<string, B> bufferSupplier;
    private readonly string testName;

    // As we add more buffer implementations/configurations, we can add them here
    // @Parameterized.Parameters(name = "{index}: test={0}")
    public static Collection<object[]> Parameters() {
        return singletonList(
            new object[] {
                "in-memory buffer",
                (Function<string, InMemoryTimeOrderedKeyValueBuffer<string, string>>) name =>
                    new InMemoryTimeOrderedKeyValueBuffer
                        .Builder<>(name, Serdes.String(), Serdes.String())
                        .build()
            }
        );
    }

    public TimeOrderedKeyValueBufferTest(string testName, Function<string, B> bufferSupplier) {
        this.testName = testName + "_" + new Random().nextInt(int.MaxValue);
        this.bufferSupplier = bufferSupplier;
    }

    private static MockInternalProcessorContext MakeContext() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        TaskId taskId = new TaskId(0, 0);

        MockInternalProcessorContext context = new MockInternalProcessorContext(properties, taskId, TestUtils.tempDirectory());
        context.setRecordCollector(new MockRecordCollector());

        return context;
    }


    private static void Cleanup(MockInternalProcessorContext context, TimeOrderedKeyValueBuffer<string, string> buffer) {
        try {
            buffer.close();
            Utils.delete(context.stateDir());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    [Xunit.Fact]
    public void ShouldInit() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);
        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldAcceptData() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);
        PutRecord(buffer, context, 0L, 0L, "asdf", "2p93nf");
        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldRejectNullValues() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);
        try {
            buffer.put(0, "asdf", null, GetContext(0));
            Assert.True(false, "expected an exception");
        } catch (NullPointerException expected) {
            // expected
        }
        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldRemoveData() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);
        PutRecord(buffer, context, 0L, 0L, "asdf", "qwer");
        Assert.Equal(buffer.numRecords(), (1));
        buffer.evictWhile(() => true, kv => { });
        Assert.Equal(buffer.numRecords(), (0));
        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldRespectEvictionPredicate() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);
        PutRecord(buffer, context, 0L, 0L, "asdf", "eyt");
        PutRecord(buffer, context, 1L, 0L, "zxcv", "rtg");
        Assert.Equal(buffer.numRecords(), (2));
        List<Eviction<string, string>> evicted = new LinkedList<>();
        buffer.evictWhile(() => buffer.numRecords() > 1, evicted::add);
        Assert.Equal(buffer.numRecords(), (1));
        Assert.Equal(evicted, (singletonList(
            new Eviction<>("asdf", new Change<>("eyt", null), GetContext(0L))
        )));
        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldTrackCount() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);
        PutRecord(buffer, context, 0L, 0L, "asdf", "oin");
        Assert.Equal(buffer.numRecords(), (1));
        PutRecord(buffer, context, 1L, 0L, "asdf", "wekjn");
        Assert.Equal(buffer.numRecords(), (1));
        PutRecord(buffer, context, 0L, 0L, "zxcv", "24inf");
        Assert.Equal(buffer.numRecords(), (2));
        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldTrackSize() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);
        PutRecord(buffer, context, 0L, 0L, "asdf", "23roni");
        Assert.Equal(buffer.bufferSize(), (43L));
        PutRecord(buffer, context, 1L, 0L, "asdf", "3l");
        Assert.Equal(buffer.bufferSize(), (39L));
        PutRecord(buffer, context, 0L, 0L, "zxcv", "qfowin");
        Assert.Equal(buffer.bufferSize(), (82L));
        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldTrackMinTimestamp() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);
        PutRecord(buffer, context, 1L, 0L, "asdf", "2093j");
        Assert.Equal(buffer.minTimestamp(), (1L));
        PutRecord(buffer, context, 0L, 0L, "zxcv", "3gon4i");
        Assert.Equal(buffer.minTimestamp(), (0L));
        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldEvictOldestAndUpdateSizeAndCountAndMinTimestamp() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);

        PutRecord(buffer, context, 1L, 0L, "zxcv", "o23i4");
        Assert.Equal(buffer.numRecords(), (1));
        Assert.Equal(buffer.bufferSize(), (42L));
        Assert.Equal(buffer.minTimestamp(), (1L));

        PutRecord(buffer, context, 0L, 0L, "asdf", "3ng");
        Assert.Equal(buffer.numRecords(), (2));
        Assert.Equal(buffer.bufferSize(), (82L));
        Assert.Equal(buffer.minTimestamp(), (0L));

        AtomicInteger callbackCount = new AtomicInteger(0);
        buffer.evictWhile(() => true, kv => {
            switch (callbackCount.incrementAndGet()) {
                case 1: {
                    Assert.Equal(kv.Key, ("asdf"));
                    Assert.Equal(buffer.numRecords(), (2));
                    Assert.Equal(buffer.bufferSize(), (82L));
                    Assert.Equal(buffer.minTimestamp(), (0L));
                    break;
                }
                case 2: {
                    Assert.Equal(kv.Key, ("zxcv"));
                    Assert.Equal(buffer.numRecords(), (1));
                    Assert.Equal(buffer.bufferSize(), (42L));
                    Assert.Equal(buffer.minTimestamp(), (1L));
                    break;
                }
                default: {
                    Assert.True(false, "too many invocations");
                    break;
                }
            }
        });
        Assert.Equal(callbackCount.get(), (2));
        Assert.Equal(buffer.numRecords(), (0));
        Assert.Equal(buffer.bufferSize(), (0L));
        Assert.Equal(buffer.minTimestamp(), (long.MaxValue));
        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldReturnUndefinedOnPriorValueForNotBufferedKey() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);

        Assert.Equal(buffer.priorValueForBuffered("ASDF"), (Maybe.undefined()));
    }

    [Xunit.Fact]
    public void ShouldReturnPriorValueForBufferedKey() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);

        ProcessorRecordContext recordContext = GetContext(0L);
        context.setRecordContext(recordContext);
        buffer.put(1L, "A", new Change<>("new-value", "old-value"), recordContext);
        buffer.put(1L, "B", new Change<>("new-value", null), recordContext);
        Assert.Equal(buffer.priorValueForBuffered("A"), (Maybe.defined(ValueAndTimestamp.make("old-value", -1))));
        Assert.Equal(buffer.priorValueForBuffered("B"), (Maybe.defined(null)));
    }

    [Xunit.Fact]
    public void ShouldFlush() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);
        PutRecord(buffer, context, 2L, 0L, "asdf", "2093j");
        PutRecord(buffer, context, 1L, 1L, "zxcv", "3gon4i");
        PutRecord(buffer, context, 0L, 2L, "deleteme", "deadbeef");

        // replace "deleteme" with a tombstone
        buffer.evictWhile(() => buffer.minTimestamp() < 1, kv => { });

        // flush everything to the changelog
        buffer.flush();

        // the buffer should serialize the buffer time and the value as byte[],
        // which we can't compare for equality using ProducerRecord.
        // As a workaround, I'm deserializing them and shoving them in a KeyValue, just for ease of testing.

        List<ProducerRecord<string, KeyValuePair<long, BufferValue>>> collected =
            ((MockRecordCollector) context.recordCollector())
                .collected()
                .stream()
                .map(pr => {
                    KeyValuePair<long, BufferValue> niceValue;
                    if (pr.Value == null) {
                        niceValue = null;
                    } else {
                        byte[] serializedValue = pr.Value;
                        ByteBuffer valueBuffer = ByteBuffer.wrap(serializedValue);
                        BufferValue contextualRecord = BufferValue.deserialize(valueBuffer);
                        long timestamp = valueBuffer.getLong();
                        niceValue = new KeyValuePair<>(timestamp, contextualRecord);
                    }

                    return new ProducerRecord<>(pr.topic(),
                                                pr.partition(),
                                                pr.Timestamp,
                                                new string(pr.Key, UTF_8),
                                                niceValue,
                                                pr.headers());
                })
                .collect(Collectors.toList());

        Assert.Equal(collected, (asList(
            new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
                                 0,   // Producer will assign
                                 null,
                                 "deleteme",
                                 null,
                                 new Headers()
            ),
            new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
                                 0,
                                 null,
                                 "zxcv",
                                 new KeyValuePair<>(1L, GetBufferValue("3gon4i", 1)),
                                 V_2_CHANGELOG_HEADERS
            ),
            new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
                                 0,
                                 null,
                                 "asdf",
                                 new KeyValuePair<>(2L, GetBufferValue("2093j", 0)),
                                 V_2_CHANGELOG_HEADERS
            )
        )));

        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldRestoreOldFormat() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);

        RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        FullChangeSerde<string> serializer = FullChangeSerde.wrap(Serdes.String());

        byte[] todeleteValue = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serializer.serializeParts(null, new Change<>("doomed", null)));
        byte[] asdfValue = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serializer.serializeParts(null, new Change<>("qwer", null)));
        byte[] zxcvValue1 = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serializer.serializeParts(null, new Change<>("eo4im", "previous")));
        byte[] zxcvValue2 = FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(serializer.serializeParts(null, new Change<>("next", "eo4im")));
        stateRestoreCallback.restoreBatch(asList(
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 0,
                                 0,
                                 TimestampType.CreateTime,
                                 -1,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 ByteBuffer.allocate(long.BYTES + todeleteValue.Length).putLong(0L).put(todeleteValue).array()),
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 1,
                                 1,
                                 TimestampType.CreateTime,
                                 -1,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 ByteBuffer.allocate(long.BYTES + asdfValue.Length).putLong(2L).put(asdfValue).array()),
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 2,
                                 2,
                                 TimestampType.CreateTime,
                                 -1,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(long.BYTES + zxcvValue1.Length).putLong(1L).put(zxcvValue1).array()),
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CreateTime,
                                 -1,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(long.BYTES + zxcvValue2.Length).putLong(1L).put(zxcvValue2).array())
        ));

        Assert.Equal(buffer.numRecords(), (3));
        Assert.Equal(buffer.minTimestamp(), (0L));
        Assert.Equal(buffer.bufferSize(), (172L));

        stateRestoreCallback.restoreBatch(singletonList(
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CreateTime,
                                 -1,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 null)
        ));

        Assert.Equal(buffer.numRecords(), (2));
        Assert.Equal(buffer.minTimestamp(), (1L));
        Assert.Equal(buffer.bufferSize(), (115L));

        Assert.Equal(buffer.priorValueForBuffered("todelete"), (Maybe.undefined()));
        Assert.Equal(buffer.priorValueForBuffered("asdf"), (Maybe.defined(null)));
        Assert.Equal(buffer.priorValueForBuffered("zxcv"), (Maybe.defined(ValueAndTimestamp.make("previous", -1))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        List<Eviction<string, string>> evicted = new LinkedList<>();
        buffer.evictWhile(() => true, evicted::add);

        // Several things to note:
        // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
        // * The record timestamps are properly restored, and not conflated with the record's buffer time.
        // * The keys and values are properly restored
        // * The record topic is set to the changelog topic. This was an oversight in the original implementation,
        //   which is fixed in changelog format v1. But upgraded applications still need to be able to handle the
        //   original format.

        Assert.Equal(evicted, (asList(
            new Eviction<>(
                "zxcv",
                new Change<>("next", "eo4im"),
                new ProcessorRecordContext(3L, 3, 0, "changelog-topic", new Headers())),
            new Eviction<>(
                "asdf",
                new Change<>("qwer", null),
                new ProcessorRecordContext(1L, 1, 0, "changelog-topic", new Headers()))
        )));

        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldRestoreV1Format() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);

        RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        Headers v1FlagHeaders = new Headers(new Header[] {new RecordHeader("v", new byte[] {(byte) 1})});

        byte[] todeleteValue = GetContextualRecord("doomed", 0).serialize(0).array();
        byte[] asdfValue = GetContextualRecord("qwer", 1).serialize(0).array();
        FullChangeSerde<string> fullChangeSerde = FullChangeSerde.wrap(Serdes.String());
        byte[] zxcvValue1 = new ContextualRecord(
            FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(fullChangeSerde.serializeParts(null, new Change<>("3o4im", "previous"))),
            GetContext(2L)
        ).serialize(0).array();
        byte[] zxcvValue2 = new ContextualRecord(
            FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(fullChangeSerde.serializeParts(null, new Change<>("next", "3o4im"))),
            GetContext(3L)
        ).serialize(0).array();
        stateRestoreCallback.restoreBatch(asList(
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 0,
                                 999,
                                 TimestampType.CreateTime,
                                 -1L,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 ByteBuffer.allocate(long.BYTES + todeleteValue.Length).putLong(0L).put(todeleteValue).array(),
                                 v1FlagHeaders),
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 1,
                                 9999,
                                 TimestampType.CreateTime,
                                 -1L,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 ByteBuffer.allocate(long.BYTES + asdfValue.Length).putLong(2L).put(asdfValue).array(),
                                 v1FlagHeaders),
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 2,
                                 99,
                                 TimestampType.CreateTime,
                                 -1L,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(long.BYTES + zxcvValue1.Length).putLong(1L).put(zxcvValue1).array(),
                                 v1FlagHeaders),
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 3,
                                 100,
                                 TimestampType.CreateTime,
                                 -1L,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(long.BYTES + zxcvValue2.Length).putLong(1L).put(zxcvValue2).array(),
                                 v1FlagHeaders)
        ));

        Assert.Equal(buffer.numRecords(), (3));
        Assert.Equal(buffer.minTimestamp(), (0L));
        Assert.Equal(buffer.bufferSize(), (142L));

        stateRestoreCallback.restoreBatch(singletonList(
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CreateTime,
                                 -1L,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 null)
        ));

        Assert.Equal(buffer.numRecords(), (2));
        Assert.Equal(buffer.minTimestamp(), (1L));
        Assert.Equal(buffer.bufferSize(), (95L));

        Assert.Equal(buffer.priorValueForBuffered("todelete"), (Maybe.undefined()));
        Assert.Equal(buffer.priorValueForBuffered("asdf"), (Maybe.defined(null)));
        Assert.Equal(buffer.priorValueForBuffered("zxcv"), (Maybe.defined(ValueAndTimestamp.make("previous", -1))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        List<Eviction<string, string>> evicted = new LinkedList<>();
        buffer.evictWhile(() => true, evicted::add);

        // Several things to note:
        // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
        // * The record timestamps are properly restored, and not conflated with the record's buffer time.
        // * The keys and values are properly restored
        // * The record topic is set to the original input topic, *not* the changelog topic
        // * The record offset preserves the original input record's offset, *not* the offset of the changelog record


        Assert.Equal(evicted, (asList(
            new Eviction<>(
                "zxcv",
                new Change<>("next", "3o4im"),
                GetContext(3L)),
            new Eviction<>(
                "asdf",
                new Change<>("qwer", null),
                GetContext(1L)
            ))));

        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldRestoreV2Format() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);

        RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        Headers v2FlagHeaders = new Headers(new Header[] {new RecordHeader("v", new byte[] {(byte) 2})});

        byte[] todeleteValue = GetBufferValue("doomed", 0).serialize(0).array();
        byte[] asdfValue = GetBufferValue("qwer", 1).serialize(0).array();
        FullChangeSerde<string> fullChangeSerde = FullChangeSerde.wrap(Serdes.String());
        byte[] zxcvValue1 =
            new BufferValue(
                Serdes.String().Serializer.serialize(null, "previous"),
                Serdes.String().Serializer.serialize(null, "IGNORED"),
                Serdes.String().Serializer.serialize(null, "3o4im"),
                GetContext(2L)
            ).serialize(0).array();
        FullChangeSerde<string> fullChangeSerde1 = FullChangeSerde.wrap(Serdes.String());
        byte[] zxcvValue2 =
            new BufferValue(
                Serdes.String().Serializer.serialize(null, "previous"),
                Serdes.String().Serializer.serialize(null, "3o4im"),
                Serdes.String().Serializer.serialize(null, "next"),
                GetContext(3L)
            ).serialize(0).array();
        stateRestoreCallback.restoreBatch(asList(
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 0,
                                 999,
                                 TimestampType.CreateTime,
                                 -1L,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 ByteBuffer.allocate(long.BYTES + todeleteValue.Length).put(todeleteValue).putLong(0L).array(),
                                 v2FlagHeaders),
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 1,
                                 9999,
                                 TimestampType.CreateTime,
                                 -1L,
                                 -1,
                                 -1,
                                 "asdf".getBytes(UTF_8),
                                 ByteBuffer.allocate(long.BYTES + asdfValue.Length).put(asdfValue).putLong(2L).array(),
                                 v2FlagHeaders),
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 2,
                                 99,
                                 TimestampType.CreateTime,
                                 -1L,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(long.BYTES + zxcvValue1.Length).put(zxcvValue1).putLong(1L).array(),
                                 v2FlagHeaders),
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 2,
                                 100,
                                 TimestampType.CreateTime,
                                 -1L,
                                 -1,
                                 -1,
                                 "zxcv".getBytes(UTF_8),
                                 ByteBuffer.allocate(long.BYTES + zxcvValue2.Length).put(zxcvValue2).putLong(1L).array(),
                                 v2FlagHeaders)
        ));

        Assert.Equal(buffer.numRecords(), (3));
        Assert.Equal(buffer.minTimestamp(), (0L));
        Assert.Equal(buffer.bufferSize(), (142L));

        stateRestoreCallback.restoreBatch(singletonList(
            new ConsumeResult<>("changelog-topic",
                                 0,
                                 3,
                                 3,
                                 TimestampType.CreateTime,
                                 -1L,
                                 -1,
                                 -1,
                                 "todelete".getBytes(UTF_8),
                                 null)
        ));

        Assert.Equal(buffer.numRecords(), (2));
        Assert.Equal(buffer.minTimestamp(), (1L));
        Assert.Equal(buffer.bufferSize(), (95L));

        Assert.Equal(buffer.priorValueForBuffered("todelete"), (Maybe.undefined()));
        Assert.Equal(buffer.priorValueForBuffered("asdf"), (Maybe.defined(null)));
        Assert.Equal(buffer.priorValueForBuffered("zxcv"), (Maybe.defined(ValueAndTimestamp.make("previous", -1))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        List<Eviction<string, string>> evicted = new LinkedList<>();
        buffer.evictWhile(() => true, evicted::add);

        // Several things to note:
        // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
        // * The record timestamps are properly restored, and not conflated with the record's buffer time.
        // * The keys and values are properly restored
        // * The record topic is set to the original input topic, *not* the changelog topic
        // * The record offset preserves the original input record's offset, *not* the offset of the changelog record


        Assert.Equal(evicted, (asList(
            new Eviction<>(
                "zxcv",
                new Change<>("next", "3o4im"),
                GetContext(3L)),
            new Eviction<>(
                "asdf",
                new Change<>("qwer", null),
                GetContext(1L)
            ))));

        Cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void ShouldNotRestoreUnrecognizedVersionRecord() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = MakeContext();
        buffer.init(context, buffer);

        RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        Headers unknownFlagHeaders = new Headers(new Header[] {new RecordHeader("v", new byte[] {(byte) -1})});

        byte[] todeleteValue = GetBufferValue("doomed", 0).serialize(0).array();
        try {
            stateRestoreCallback.restoreBatch(singletonList(
                new ConsumeResult<>("changelog-topic",
                                     0,
                                     0,
                                     999,
                                     TimestampType.CreateTime,
                                     -1L,
                                     -1,
                                     -1,
                                     "todelete".getBytes(UTF_8),
                                     ByteBuffer.allocate(long.BYTES + todeleteValue.Length).putLong(0L).put(todeleteValue).array(),
                                     unknownFlagHeaders)
            ));
            Assert.True(false, "expected an exception");
        } catch (IllegalArgumentException expected) {
            // nothing to do.
        } finally {
            Cleanup(context, buffer);
        }
    }

    private static void PutRecord(TimeOrderedKeyValueBuffer<string, string> buffer,
                                  MockInternalProcessorContext context,
                                  long streamTime,
                                  long recordTimestamp,
                                  string key,
                                  string value) {
        ProcessorRecordContext recordContext = GetContext(recordTimestamp);
        context.setRecordContext(recordContext);
        buffer.put(streamTime, key, new Change<>(value, null), recordContext);
    }

    private static BufferValue GetBufferValue(string value, long timestamp) {
        return new BufferValue(
            null,
            null,
            Serdes.String().Serializer.serialize(null, value),
            GetContext(timestamp)
        );
    }

    private static ContextualRecord GetContextualRecord(string value, long timestamp) {
        FullChangeSerde<string> fullChangeSerde = FullChangeSerde.wrap(Serdes.String());
        return new ContextualRecord(
            FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(fullChangeSerde.serializeParts(null, new Change<>(value, null))),
            GetContext(timestamp)
        );
    }

    private static ProcessorRecordContext GetContext(long recordTimestamp) {
        return new ProcessorRecordContext(recordTimestamp, 0, 0, "topic", null);
    }
}
