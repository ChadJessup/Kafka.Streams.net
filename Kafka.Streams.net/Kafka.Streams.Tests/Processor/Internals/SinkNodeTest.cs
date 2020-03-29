/*






 *

 *





 */




















public class SinkNodeTest {
    private Serializer<byte[]> anySerializer = Serdes.ByteArray().Serializer;
    private StateSerdes<Bytes, Bytes> anyStateSerde = StateSerdes.withBuiltinTypes("anyName", Bytes, Bytes);
    private RecordCollector recordCollector =  new RecordCollectorImpl(
        null,
        new LogContext("sinknode-test "),
        new DefaultProductionExceptionHandler(),
        new Metrics().sensor("skipped-records")
    );

    private InternalMockProcessorContext context = new InternalMockProcessorContext(
        anyStateSerde,
        recordCollector
    );
    private SinkNode<byte[], byte[]> sink = new SinkNode<>("anyNodeName",
            new StaticTopicNameExtractor<>("any-output-topic"), anySerializer, anySerializer, null);

    // Used to verify that the correct exceptions are thrown if the compiler checks are bypassed
    
    private SinkNode<object, object> illTypedSink = (SinkNode) sink;

    
    public void before() {
        recordCollector.init(new MockProducer<>(true, anySerializer, anySerializer));
        sink.init(context);
    }

    [Xunit.Fact]
    public void shouldThrowStreamsExceptionOnInputRecordWithInvalidTimestamp() {
        Bytes anyKey = new Bytes("any key".getBytes());
        Bytes anyValue = new Bytes("any value".getBytes());

        // When/Then
        context.setTime(-1); // ensures a negative timestamp is set for the record we send next
        try {
            illTypedSink.process(anyKey, anyValue);
            Assert.True(false, "Should have thrown StreamsException");
        } catch (StreamsException ignored) {
            // expected
        }
    }

    [Xunit.Fact]
    public void shouldThrowStreamsExceptionOnKeyValueTypeSerializerMismatch() {
        string keyOfDifferentTypeThanSerializer = "key with different type";
        string valueOfDifferentTypeThanSerializer = "value with different type";

        // When/Then
        context.setTime(0);
        try {
            illTypedSink.process(keyOfDifferentTypeThanSerializer, valueOfDifferentTypeThanSerializer);
            Assert.True(false, "Should have thrown StreamsException");
        } catch (StreamsException e) {
            Assert.Equal(e.getCause(), instanceOf(ClassCastException));
        }
    }

    [Xunit.Fact]
    public void shouldHandleNullKeysWhenThrowingStreamsExceptionOnKeyValueTypeSerializerMismatch() {
        string invalidValueToTriggerSerializerMismatch = "";

        // When/Then
        context.setTime(1);
        try {
            illTypedSink.process(null, invalidValueToTriggerSerializerMismatch);
            Assert.True(false, "Should have thrown StreamsException");
        } catch (StreamsException e) {
            Assert.Equal(e.getCause(), instanceOf(ClassCastException));
            Assert.Equal(e.getMessage(), containsString("unknown because key is null"));
        }
    }

    [Xunit.Fact]
    public void shouldHandleNullValuesWhenThrowingStreamsExceptionOnKeyValueTypeSerializerMismatch() {
        string invalidKeyToTriggerSerializerMismatch = "";

        // When/Then
        context.setTime(1);
        try {
            illTypedSink.process(invalidKeyToTriggerSerializerMismatch, null);
            Assert.True(false, "Should have thrown StreamsException");
        } catch (StreamsException e) {
            Assert.Equal(e.getCause(), instanceOf(ClassCastException));
            Assert.Equal(e.getMessage(), containsString("unknown because value is null"));
        }
    }

}
