/*






 *

 *





 */













































public class TimeOrderedKeyValueBufferTest<B : TimeOrderedKeyValueBuffer<string, string>> {
    private static Headers V_2_CHANGELOG_HEADERS =
        new Headers(new Header[] {new RecordHeader("v", new byte[] {(byte) 2})});

    private static string APP_ID = "test-app";
    private Function<string, B> bufferSupplier;
    private string testName;

    // As we add more buffer implementations/configurations, we can add them here
    // @Parameterized.Parameters(name = "{index}: test={0}")
    public static Collection<object[]> parameters() {
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

    private static MockInternalProcessorContext makeContext() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        TaskId taskId = new TaskId(0, 0);

        MockInternalProcessorContext context = new MockInternalProcessorContext(properties, taskId, TestUtils.tempDirectory());
        context.setRecordCollector(new MockRecordCollector());

        return context;
    }


    private static void cleanup(MockInternalProcessorContext context, TimeOrderedKeyValueBuffer<string, string> buffer) {
        try {
            buffer.close();
            Utils.delete(context.stateDir());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    [Xunit.Fact]
    public void shouldInit() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldAcceptData() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "2p93nf");
        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldRejectNullValues() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        try {
            buffer.put(0, "asdf", null, getContext(0));
            Assert.True(false, "expected an exception");
        } catch (NullPointerException expected) {
            // expected
        }
        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldRemoveData() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "qwer");
        Assert.Equal(buffer.numRecords(), is(1));
        buffer.evictWhile(() => true, kv => { });
        Assert.Equal(buffer.numRecords(), is(0));
        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldRespectEvictionPredicate() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "eyt");
        putRecord(buffer, context, 1L, 0L, "zxcv", "rtg");
        Assert.Equal(buffer.numRecords(), is(2));
        List<Eviction<string, string>> evicted = new LinkedList<>();
        buffer.evictWhile(() => buffer.numRecords() > 1, evicted::add);
        Assert.Equal(buffer.numRecords(), is(1));
        Assert.Equal(evicted, is(singletonList(
            new Eviction<>("asdf", new Change<>("eyt", null), getContext(0L))
        )));
        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldTrackCount() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "oin");
        Assert.Equal(buffer.numRecords(), is(1));
        putRecord(buffer, context, 1L, 0L, "asdf", "wekjn");
        Assert.Equal(buffer.numRecords(), is(1));
        putRecord(buffer, context, 0L, 0L, "zxcv", "24inf");
        Assert.Equal(buffer.numRecords(), is(2));
        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldTrackSize() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 0L, 0L, "asdf", "23roni");
        Assert.Equal(buffer.bufferSize(), is(43L));
        putRecord(buffer, context, 1L, 0L, "asdf", "3l");
        Assert.Equal(buffer.bufferSize(), is(39L));
        putRecord(buffer, context, 0L, 0L, "zxcv", "qfowin");
        Assert.Equal(buffer.bufferSize(), is(82L));
        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldTrackMinTimestamp() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 1L, 0L, "asdf", "2093j");
        Assert.Equal(buffer.minTimestamp(), is(1L));
        putRecord(buffer, context, 0L, 0L, "zxcv", "3gon4i");
        Assert.Equal(buffer.minTimestamp(), is(0L));
        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldEvictOldestAndUpdateSizeAndCountAndMinTimestamp() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        putRecord(buffer, context, 1L, 0L, "zxcv", "o23i4");
        Assert.Equal(buffer.numRecords(), is(1));
        Assert.Equal(buffer.bufferSize(), is(42L));
        Assert.Equal(buffer.minTimestamp(), is(1L));

        putRecord(buffer, context, 0L, 0L, "asdf", "3ng");
        Assert.Equal(buffer.numRecords(), is(2));
        Assert.Equal(buffer.bufferSize(), is(82L));
        Assert.Equal(buffer.minTimestamp(), is(0L));

        AtomicInteger callbackCount = new AtomicInteger(0);
        buffer.evictWhile(() => true, kv => {
            switch (callbackCount.incrementAndGet()) {
                case 1: {
                    Assert.Equal(kv.Key, is("asdf"));
                    Assert.Equal(buffer.numRecords(), is(2));
                    Assert.Equal(buffer.bufferSize(), is(82L));
                    Assert.Equal(buffer.minTimestamp(), is(0L));
                    break;
                }
                case 2: {
                    Assert.Equal(kv.Key, is("zxcv"));
                    Assert.Equal(buffer.numRecords(), is(1));
                    Assert.Equal(buffer.bufferSize(), is(42L));
                    Assert.Equal(buffer.minTimestamp(), is(1L));
                    break;
                }
                default: {
                    Assert.True(false, "too many invocations");
                    break;
                }
            }
        });
        Assert.Equal(callbackCount.get(), is(2));
        Assert.Equal(buffer.numRecords(), is(0));
        Assert.Equal(buffer.bufferSize(), is(0L));
        Assert.Equal(buffer.minTimestamp(), is(long.MaxValue));
        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldReturnUndefinedOnPriorValueForNotBufferedKey() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        Assert.Equal(buffer.priorValueForBuffered("ASDF"), is(Maybe.undefined()));
    }

    [Xunit.Fact]
    public void shouldReturnPriorValueForBufferedKey() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        ProcessorRecordContext recordContext = getContext(0L);
        context.setRecordContext(recordContext);
        buffer.put(1L, "A", new Change<>("new-value", "old-value"), recordContext);
        buffer.put(1L, "B", new Change<>("new-value", null), recordContext);
        Assert.Equal(buffer.priorValueForBuffered("A"), is(Maybe.defined(ValueAndTimestamp.make("old-value", -1))));
        Assert.Equal(buffer.priorValueForBuffered("B"), is(Maybe.defined(null)));
    }

    [Xunit.Fact]
    public void shouldFlush() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);
        putRecord(buffer, context, 2L, 0L, "asdf", "2093j");
        putRecord(buffer, context, 1L, 1L, "zxcv", "3gon4i");
        putRecord(buffer, context, 0L, 2L, "deleteme", "deadbeef");

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

        Assert.Equal(collected, is(asList(
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
                                 new KeyValuePair<>(1L, getBufferValue("3gon4i", 1)),
                                 V_2_CHANGELOG_HEADERS
            ),
            new ProducerRecord<>(APP_ID + "-" + testName + "-changelog",
                                 0,
                                 null,
                                 "asdf",
                                 new KeyValuePair<>(2L, getBufferValue("2093j", 0)),
                                 V_2_CHANGELOG_HEADERS
            )
        )));

        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldRestoreOldFormat() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
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

        Assert.Equal(buffer.numRecords(), is(3));
        Assert.Equal(buffer.minTimestamp(), is(0L));
        Assert.Equal(buffer.bufferSize(), is(172L));

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

        Assert.Equal(buffer.numRecords(), is(2));
        Assert.Equal(buffer.minTimestamp(), is(1L));
        Assert.Equal(buffer.bufferSize(), is(115L));

        Assert.Equal(buffer.priorValueForBuffered("todelete"), is(Maybe.undefined()));
        Assert.Equal(buffer.priorValueForBuffered("asdf"), is(Maybe.defined(null)));
        Assert.Equal(buffer.priorValueForBuffered("zxcv"), is(Maybe.defined(ValueAndTimestamp.make("previous", -1))));

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

        Assert.Equal(evicted, is(asList(
            new Eviction<>(
                "zxcv",
                new Change<>("next", "eo4im"),
                new ProcessorRecordContext(3L, 3, 0, "changelog-topic", new Headers())),
            new Eviction<>(
                "asdf",
                new Change<>("qwer", null),
                new ProcessorRecordContext(1L, 1, 0, "changelog-topic", new Headers()))
        )));

        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldRestoreV1Format() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        Headers v1FlagHeaders = new Headers(new Header[] {new RecordHeader("v", new byte[] {(byte) 1})});

        byte[] todeleteValue = getContextualRecord("doomed", 0).serialize(0).array();
        byte[] asdfValue = getContextualRecord("qwer", 1).serialize(0).array();
        FullChangeSerde<string> fullChangeSerde = FullChangeSerde.wrap(Serdes.String());
        byte[] zxcvValue1 = new ContextualRecord(
            FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(fullChangeSerde.serializeParts(null, new Change<>("3o4im", "previous"))),
            getContext(2L)
        ).serialize(0).array();
        byte[] zxcvValue2 = new ContextualRecord(
            FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(fullChangeSerde.serializeParts(null, new Change<>("next", "3o4im"))),
            getContext(3L)
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

        Assert.Equal(buffer.numRecords(), is(3));
        Assert.Equal(buffer.minTimestamp(), is(0L));
        Assert.Equal(buffer.bufferSize(), is(142L));

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

        Assert.Equal(buffer.numRecords(), is(2));
        Assert.Equal(buffer.minTimestamp(), is(1L));
        Assert.Equal(buffer.bufferSize(), is(95L));

        Assert.Equal(buffer.priorValueForBuffered("todelete"), is(Maybe.undefined()));
        Assert.Equal(buffer.priorValueForBuffered("asdf"), is(Maybe.defined(null)));
        Assert.Equal(buffer.priorValueForBuffered("zxcv"), is(Maybe.defined(ValueAndTimestamp.make("previous", -1))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        List<Eviction<string, string>> evicted = new LinkedList<>();
        buffer.evictWhile(() => true, evicted::add);

        // Several things to note:
        // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
        // * The record timestamps are properly restored, and not conflated with the record's buffer time.
        // * The keys and values are properly restored
        // * The record topic is set to the original input topic, *not* the changelog topic
        // * The record offset preserves the original input record's offset, *not* the offset of the changelog record


        Assert.Equal(evicted, is(asList(
            new Eviction<>(
                "zxcv",
                new Change<>("next", "3o4im"),
                getContext(3L)),
            new Eviction<>(
                "asdf",
                new Change<>("qwer", null),
                getContext(1L)
            ))));

        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldRestoreV2Format() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        Headers v2FlagHeaders = new Headers(new Header[] {new RecordHeader("v", new byte[] {(byte) 2})});

        byte[] todeleteValue = getBufferValue("doomed", 0).serialize(0).array();
        byte[] asdfValue = getBufferValue("qwer", 1).serialize(0).array();
        FullChangeSerde<string> fullChangeSerde = FullChangeSerde.wrap(Serdes.String());
        byte[] zxcvValue1 =
            new BufferValue(
                Serdes.String().Serializer.serialize(null, "previous"),
                Serdes.String().Serializer.serialize(null, "IGNORED"),
                Serdes.String().Serializer.serialize(null, "3o4im"),
                getContext(2L)
            ).serialize(0).array();
        FullChangeSerde<string> fullChangeSerde1 = FullChangeSerde.wrap(Serdes.String());
        byte[] zxcvValue2 =
            new BufferValue(
                Serdes.String().Serializer.serialize(null, "previous"),
                Serdes.String().Serializer.serialize(null, "3o4im"),
                Serdes.String().Serializer.serialize(null, "next"),
                getContext(3L)
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

        Assert.Equal(buffer.numRecords(), is(3));
        Assert.Equal(buffer.minTimestamp(), is(0L));
        Assert.Equal(buffer.bufferSize(), is(142L));

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

        Assert.Equal(buffer.numRecords(), is(2));
        Assert.Equal(buffer.minTimestamp(), is(1L));
        Assert.Equal(buffer.bufferSize(), is(95L));

        Assert.Equal(buffer.priorValueForBuffered("todelete"), is(Maybe.undefined()));
        Assert.Equal(buffer.priorValueForBuffered("asdf"), is(Maybe.defined(null)));
        Assert.Equal(buffer.priorValueForBuffered("zxcv"), is(Maybe.defined(ValueAndTimestamp.make("previous", -1))));

        // flush the buffer into a list in buffer order so we can make assertions about the contents.

        List<Eviction<string, string>> evicted = new LinkedList<>();
        buffer.evictWhile(() => true, evicted::add);

        // Several things to note:
        // * The buffered records are ordered according to their buffer time (serialized in the value of the changelog)
        // * The record timestamps are properly restored, and not conflated with the record's buffer time.
        // * The keys and values are properly restored
        // * The record topic is set to the original input topic, *not* the changelog topic
        // * The record offset preserves the original input record's offset, *not* the offset of the changelog record


        Assert.Equal(evicted, is(asList(
            new Eviction<>(
                "zxcv",
                new Change<>("next", "3o4im"),
                getContext(3L)),
            new Eviction<>(
                "asdf",
                new Change<>("qwer", null),
                getContext(1L)
            ))));

        cleanup(context, buffer);
    }

    [Xunit.Fact]
    public void shouldNotRestoreUnrecognizedVersionRecord() {
        TimeOrderedKeyValueBuffer<string, string> buffer = bufferSupplier.apply(testName);
        MockInternalProcessorContext context = makeContext();
        buffer.init(context, buffer);

        RecordBatchingStateRestoreCallback stateRestoreCallback =
            (RecordBatchingStateRestoreCallback) context.stateRestoreCallback(testName);

        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, "", null));

        Headers unknownFlagHeaders = new Headers(new Header[] {new RecordHeader("v", new byte[] {(byte) -1})});

        byte[] todeleteValue = getBufferValue("doomed", 0).serialize(0).array();
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
            cleanup(context, buffer);
        }
    }

    private static void putRecord(TimeOrderedKeyValueBuffer<string, string> buffer,
                                  MockInternalProcessorContext context,
                                  long streamTime,
                                  long recordTimestamp,
                                  string key,
                                  string value) {
        ProcessorRecordContext recordContext = getContext(recordTimestamp);
        context.setRecordContext(recordContext);
        buffer.put(streamTime, key, new Change<>(value, null), recordContext);
    }

    private static BufferValue getBufferValue(string value, long timestamp) {
        return new BufferValue(
            null,
            null,
            Serdes.String().Serializer.serialize(null, value),
            getContext(timestamp)
        );
    }

    private static ContextualRecord getContextualRecord(string value, long timestamp) {
        FullChangeSerde<string> fullChangeSerde = FullChangeSerde.wrap(Serdes.String());
        return new ContextualRecord(
            FullChangeSerde.mergeChangeArraysIntoSingleLegacyFormattedArray(fullChangeSerde.serializeParts(null, new Change<>(value, null))),
            getContext(timestamp)
        );
    }

    private static ProcessorRecordContext getContext(long recordTimestamp) {
        return new ProcessorRecordContext(recordTimestamp, 0, 0, "topic", null);
    }
}
