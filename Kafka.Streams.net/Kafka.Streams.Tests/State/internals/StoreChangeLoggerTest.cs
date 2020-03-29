/*






 *

 *





 */






















public class StoreChangeLoggerTest {

    private string topic = "topic";

    private Dictionary<int, ValueAndTimestamp<string>> logged = new HashMap<>();
    private Dictionary<int, Headers> loggedHeaders = new HashMap<>();

    private InternalMockProcessorContext context = new InternalMockProcessorContext(
        StateSerdes.withBuiltinTypes(topic, int, string),
        new RecordCollectorImpl(
            "StoreChangeLoggerTest",
            new LogContext("StoreChangeLoggerTest "),
            new DefaultProductionExceptionHandler(),
            new Metrics().sensor("skipped-records")) {

            
            public void Send<K1, V1>(string topic,
                                      K1 key,
                                      V1 value,
                                      Headers headers,
                                      int partition,
                                      long timestamp,
                                      Serializer<K1> keySerializer,
                                      Serializer<V1> valueSerializer) {
                logged.put((int) key, ValueAndTimestamp.make((string) value, timestamp));
                loggedHeaders.put((int) key, headers);
            }

            
            public void Send<K1, V1>(string topic,
                                      K1 key,
                                      V1 value,
                                      Headers headers,
                                      long timestamp,
                                      Serializer<K1> keySerializer,
                                      Serializer<V1> valueSerializer,
                                      StreamPartitioner<? super K1, ? super V1> partitioner) {
                throw new UnsupportedOperationException();
            }
        }
    );

    private StoreChangeLogger<int, string> changeLogger =
        new StoreChangeLogger<>(topic, context, StateSerdes.withBuiltinTypes(topic, int, string));

    [Xunit.Fact]
    public void TestAddRemove() {
        context.setTime(1);
        changeLogger.logChange(0, "zero");
        context.setTime(5);
        changeLogger.logChange(1, "one");
        changeLogger.logChange(2, "two");
        changeLogger.logChange(3, "three", 42L);

        Assert.Equal(ValueAndTimestamp.make("zero", 1L), logged.get(0));
        Assert.Equal(ValueAndTimestamp.make("one", 5L), logged.get(1));
        Assert.Equal(ValueAndTimestamp.make("two", 5L), logged.get(2));
        Assert.Equal(ValueAndTimestamp.make("three", 42L), logged.get(3));

        changeLogger.logChange(0, null);
        assertNull(logged.get(0));
    }

    [Xunit.Fact]
    public void ShouldNotSendRecordHeadersToChangelogTopic() {
        context.headers().add(new RecordHeader("key", "value".getBytes()));
        changeLogger.logChange(0, "zero");
        changeLogger.logChange(0, "zero", 42L);

        assertNull(loggedHeaders.get(0));
    }
}
