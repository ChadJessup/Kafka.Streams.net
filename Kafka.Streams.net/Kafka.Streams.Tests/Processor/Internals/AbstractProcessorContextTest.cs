/*






 *

 *





 */






























public class AbstractProcessorContextTest {

    private MockStreamsMetrics metrics = new MockStreamsMetrics(new Metrics());
    private AbstractProcessorContext context = new TestProcessorContext(metrics);
    private MockKeyValueStore stateStore = new MockKeyValueStore("store", false);
    private Headers headers = new Headers(new Header[]{new RecordHeader("key", "value".getBytes())});
    private ProcessorRecordContext recordContext = new ProcessorRecordContext(10, System.currentTimeMillis(), 1, "foo", headers);

    
    public void before() {
        context.setRecordContext(recordContext);
    }

    [Xunit.Fact]
    public void shouldThrowIllegalStateExceptionOnRegisterWhenContextIsInitialized() {
        context.initialize();
        try {
            context.register(stateStore, null);
            Assert.True(false, "should throw illegal state exception when context already initialized");
        } catch (IllegalStateException e) {
            // pass
        }
    }

    [Xunit.Fact]
    public void shouldNotThrowIllegalStateExceptionOnRegisterWhenContextIsNotInitialized() {
        context.register(stateStore, null);
    }

    [Xunit.Fact]// (expected = NullPointerException)
    public void shouldThrowNullPointerOnRegisterIfStateStoreIsNull() {
        context.register(null, null);
    }

    [Xunit.Fact]
    public void shouldThrowIllegalStateExceptionOnTopicIfNoRecordContext() {
        context.setRecordContext(null);
        try {
            context.topic();
            Assert.True(false, "should throw illegal state exception when record context is null");
        } catch (IllegalStateException e) {
            // pass
        }
    }

    [Xunit.Fact]
    public void shouldReturnTopicFromRecordContext() {
        Assert.Equal(context.topic(), (recordContext.topic()));
    }

    [Xunit.Fact]
    public void shouldReturnNullIfTopicEqualsNonExistTopic() {
        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, AbstractProcessorContext.NONEXIST_TOPIC, null));
        Assert.Equal(context.topic(), nullValue());
    }

    [Xunit.Fact]
    public void shouldThrowIllegalStateExceptionOnPartitionIfNoRecordContext() {
        context.setRecordContext(null);
        try {
            context.partition();
            Assert.True(false, "should throw illegal state exception when record context is null");
        } catch (IllegalStateException e) {
            // pass
        }
    }

    [Xunit.Fact]
    public void shouldReturnPartitionFromRecordContext() {
        Assert.Equal(context.partition(), (recordContext.partition()));
    }

    [Xunit.Fact]
    public void shouldThrowIllegalStateExceptionOnOffsetIfNoRecordContext() {
        context.setRecordContext(null);
        try {
            context.Offset;
        } catch (IllegalStateException e) {
            // pass
        }
    }

    [Xunit.Fact]
    public void shouldReturnOffsetFromRecordContext() {
        Assert.Equal(context.Offset, (recordContext.Offset));
    }

    [Xunit.Fact]
    public void shouldThrowIllegalStateExceptionOnTimestampIfNoRecordContext() {
        context.setRecordContext(null);
        try {
            context.Timestamp;
            Assert.True(false, "should throw illegal state exception when record context is null");
        } catch (IllegalStateException e) {
            // pass
        }
    }

    [Xunit.Fact]
    public void shouldReturnTimestampFromRecordContext() {
        Assert.Equal(context.Timestamp, (recordContext.Timestamp));
    }

    [Xunit.Fact]
    public void shouldReturnHeadersFromRecordContext() {
        Assert.Equal(context.headers(), (recordContext.headers()));
    }

    [Xunit.Fact]
    public void shouldReturnNullIfHeadersAreNotSet() {
        context.setRecordContext(new ProcessorRecordContext(0, 0, 0, AbstractProcessorContext.NONEXIST_TOPIC, null));
        Assert.Equal(context.headers(), nullValue());
    }

    [Xunit.Fact]
    public void shouldThrowIllegalStateExceptionOnHeadersIfNoRecordContext() {
        context.setRecordContext(null);
        try {
            context.headers();
        } catch (IllegalStateException e) {
            // pass
        }
    }

    
    [Xunit.Fact]
    public void appConfigsShouldReturnParsedValues() {
        Assert.Equal(
            context.appConfigs().get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG),
            equalTo(RocksDBConfigSetter));
    }

    [Xunit.Fact]
    public void appConfigsShouldReturnUnrecognizedValues() {
        Assert.Equal(
            context.appConfigs().get("user.supplied.config"),
            equalTo("user-suppplied-value"));
    }


    private static class TestProcessorContext : AbstractProcessorContext {
        static Properties config;
        static {
            config = getStreamsConfig();
            // Value must be a string to test className => class conversion
            config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBConfigSetter.getName());
            config.put("user.supplied.config", "user-suppplied-value");
        }

        TestProcessorContext(MockStreamsMetrics metrics) {
            super(new TaskId(0, 0), new StreamsConfig(config), metrics, new StateManagerStub(), new ThreadCache(new LogContext("name "), 0, metrics));
        }

        
        public StateStore getStateStore(string name) {
            return null;
        }

        
        @Deprecated
        public Cancellable schedule(long interval,
                                    PunctuationType type,
                                    Punctuator callback) {
            return null;
        }

        
        public Cancellable schedule(Duration interval,
                                    PunctuationType type,
                                    Punctuator callback) {// throws IllegalArgumentException
            return null;
        }

        
        public void forward<K, V>(K key, V value) {}

        
        public void forward<K, V>(K key, V value, To to) {}

        
        @Deprecated
        public void forward<K, V>(K key, V value, int childIndex) {}

        
        @Deprecated
        public void forward<K, V>(K key, V value, string childName) {}

        
        public void commit() {}
    }
}
