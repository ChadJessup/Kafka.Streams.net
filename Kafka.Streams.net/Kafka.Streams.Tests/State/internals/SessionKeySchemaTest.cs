/*






 *

 *





 */

























public class SessionKeySchemaTest {

    private string key = "key";
    private string topic = "topic";
    private long startTime = 50L;
    private long endTime = 100L;
    private Serde<string> serde = Serdes.String();

    private Window window = new SessionWindow(startTime, endTime);
    private Windowed<string> windowedKey = new Windowed<>(key, window);
    private Serde<Windowed<string>> keySerde = new WindowedSerdes.SessionWindowedSerde<>(serde);

    private SessionKeySchema sessionKeySchema = new SessionKeySchema();
    private DelegatingPeekingKeyValueIterator<Bytes, int> iterator;

    
    public void before() {
        List<KeyValuePair<Bytes, int>> keys = Array.asList(KeyValuePair.Create(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0}), new SessionWindow(0, 0))), 1),
                                                                  KeyValuePair.Create(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0}), new SessionWindow(0, 0))), 2),
                                                                  KeyValuePair.Create(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0, 0}), new SessionWindow(0, 0))), 3),
                                                                  KeyValuePair.Create(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0}), new SessionWindow(10, 20))), 4),
                                                                  KeyValuePair.Create(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0}), new SessionWindow(10, 20))), 5),
                                                                  KeyValuePair.Create(SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0, 0, 0}), new SessionWindow(10, 20))), 6));
        iterator = new DelegatingPeekingKeyValueIterator<>("foo", new KeyValueIteratorStub<>(keys.iterator()));
    }

    [Xunit.Fact]
    public void shouldFetchExactKeysSkippingLongerKeys() {
        Bytes key = Bytes.wrap(new byte[]{0});
        List<int> result = getValues(sessionKeySchema.hasNextCondition(key, key, 0, long.MaxValue));
        Assert.Equal(result, (Array.asList(2, 4)));
    }

    [Xunit.Fact]
    public void shouldFetchExactKeySkippingShorterKeys() {
        Bytes key = Bytes.wrap(new byte[]{0, 0});
        HasNextCondition hasNextCondition = sessionKeySchema.hasNextCondition(key, key, 0, long.MaxValue);
        List<int> results = getValues(hasNextCondition);
        Assert.Equal(results, (Array.asList(1, 5)));
    }

    [Xunit.Fact]
    public void shouldFetchAllKeysUsingNullKeys() {
        HasNextCondition hasNextCondition = sessionKeySchema.hasNextCondition(null, null, 0, long.MaxValue);
        List<int> results = getValues(hasNextCondition);
        Assert.Equal(results, (Array.asList(1, 2, 3, 4, 5, 6)));
    }
    
    [Xunit.Fact]
    public void testUpperBoundWithLargeTimestamps() {
        Bytes upper = sessionKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), long.MaxValue);

        Assert.Equal(
            "shorter key with max timestamp should be in range",
            upper.compareTo(SessionKeySchema.toBinary(
                new Windowed<>(
                    Bytes.wrap(new byte[]{0xA}),
                    new SessionWindow(long.MaxValue, long.MaxValue))
            )) >= 0
        );

        Assert.Equal(
            "shorter key with max timestamp should be in range",
            upper.compareTo(SessionKeySchema.toBinary(
                new Windowed<>(
                    Bytes.wrap(new byte[]{0xA, 0xB}),
                    new SessionWindow(long.MaxValue, long.MaxValue))

            )) >= 0
        );

        Assert.Equal(upper, (SessionKeySchema.toBinary(
            new Windowed<>(Bytes.wrap(new byte[]{0xA}), new SessionWindow(long.MaxValue, long.MaxValue))))
        );
    }

    [Xunit.Fact]
    public void testUpperBoundWithKeyBytesLargerThanFirstTimestampByte() {
        Bytes upper = sessionKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}), long.MaxValue);

        Assert.Equal(
            "shorter key with max timestamp should be in range",
            upper.compareTo(SessionKeySchema.toBinary(
                new Windowed<>(
                    Bytes.wrap(new byte[]{0xA, (byte) 0x8F}),
                    new SessionWindow(long.MaxValue, long.MaxValue))
                )
            ) >= 0
        );

        Assert.Equal(upper, (SessionKeySchema.toBinary(
            new Windowed<>(Bytes.wrap(new byte[]{0xA, (byte) 0x8F, (byte) 0x9F}), new SessionWindow(long.MaxValue, long.MaxValue))))
        );
    }

    [Xunit.Fact]
    public void testUpperBoundWithZeroTimestamp() {
        Bytes upper = sessionKeySchema.upperRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);

        Assert.Equal(upper, (SessionKeySchema.toBinary(
            new Windowed<>(Bytes.wrap(new byte[]{0xA}), new SessionWindow(0, long.MaxValue))))
        );
    }

    [Xunit.Fact]
    public void testLowerBoundWithZeroTimestamp() {
        Bytes lower = sessionKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), 0);
        Assert.Equal(lower, (SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), new SessionWindow(0, 0)))));
    }

    [Xunit.Fact]
    public void testLowerBoundMatchesTrailingZeros() {
        Bytes lower = sessionKeySchema.lowerRange(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), long.MaxValue);

        Assert.Equal(
            "appending zeros to key should still be in range",
            lower.compareTo(SessionKeySchema.toBinary(
                new Windowed<>(
                    Bytes.wrap(new byte[]{0xA, 0xB, 0xC, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
                    new SessionWindow(long.MaxValue, long.MaxValue))
            )) < 0
        );

        Assert.Equal(lower, (SessionKeySchema.toBinary(new Windowed<>(Bytes.wrap(new byte[]{0xA, 0xB, 0xC}), new SessionWindow(0, 0)))));
    }

    [Xunit.Fact]
    public void shouldSerializeDeserialize() {
        byte[] bytes = keySerde.Serializer.serialize(topic, windowedKey);
        Windowed<string> result = keySerde.deserializer().deserialize(topic, bytes);
        Assert.Equal(windowedKey, result);
    }

    [Xunit.Fact]
    public void shouldSerializeNullToNull() {
        assertNull(keySerde.Serializer.serialize(topic, null));
    }

    [Xunit.Fact]
    public void shouldDeSerializeEmtpyByteArrayToNull() {
        assertNull(keySerde.deserializer().deserialize(topic, new byte[0]));
    }

    [Xunit.Fact]
    public void shouldDeSerializeNullToNull() {
        assertNull(keySerde.deserializer().deserialize(topic, null));
    }

    [Xunit.Fact]
    public void shouldConvertToBinaryAndBack() {
        byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.Serializer, "dummy");
        Windowed<string> result = SessionKeySchema.from(serialized, Serdes.String().deserializer(), "dummy");
        Assert.Equal(windowedKey, result);
    }

    [Xunit.Fact]
    public void shouldExtractEndTimeFromBinary() {
        byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.Serializer, "dummy");
        Assert.Equal(endTime, SessionKeySchema.extractEndTimestamp(serialized));
    }

    [Xunit.Fact]
    public void shouldExtractStartTimeFromBinary() {
        byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.Serializer, "dummy");
        Assert.Equal(startTime, SessionKeySchema.extractStartTimestamp(serialized));
    }

    [Xunit.Fact]
    public void shouldExtractWindowFromBindary() {
        byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.Serializer, "dummy");
        Assert.Equal(window, SessionKeySchema.extractWindow(serialized));
    }

    [Xunit.Fact]
    public void shouldExtractKeyBytesFromBinary() {
        byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.Serializer, "dummy");
        assertArrayEquals(key.getBytes(), SessionKeySchema.extractKeyBytes(serialized));
    }

    [Xunit.Fact]
    public void shouldExtractKeyFromBinary() {
        byte[] serialized = SessionKeySchema.toBinary(windowedKey, serde.Serializer, "dummy");
        Assert.Equal(windowedKey, SessionKeySchema.from(serialized, serde.deserializer(), "dummy"));
    }

    [Xunit.Fact]
    public void shouldExtractBytesKeyFromBinary() {
        Bytes bytesKey = Bytes.wrap(key.getBytes());
        Windowed<Bytes> windowedBytesKey = new Windowed<>(bytesKey, window);
        Bytes serialized = SessionKeySchema.toBinary(windowedBytesKey);
        Assert.Equal(windowedBytesKey, SessionKeySchema.from(serialized));
    }

    private List<int> getValues(HasNextCondition hasNextCondition) {
        List<int> results = new ArrayList<>();
        while (hasNextCondition.hasNext(iterator)) {
            results.add(iterator.next().value);
        }
        return results;
    }

}
