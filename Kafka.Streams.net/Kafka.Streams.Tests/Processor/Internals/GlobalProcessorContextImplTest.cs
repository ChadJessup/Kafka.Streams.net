/*






 *

 *





 */



























public class GlobalProcessorContextImplTest {
    private static string GLOBAL_STORE_NAME = "global-store";
    private static string GLOBAL_KEY_VALUE_STORE_NAME = "global-key-value-store";
    private static string GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME = "global-timestamped-key-value-store";
    private static string GLOBAL_WINDOW_STORE_NAME = "global-window-store";
    private static string GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME = "global-timestamped-window-store";
    private static string GLOBAL_SESSION_STORE_NAME = "global-session-store";
    private static string UNKNOWN_STORE = "unknown-store";
    private static string CHILD_PROCESSOR = "child";

    private GlobalProcessorContextImpl globalContext;

    private ProcessorNode child;
    private ProcessorRecordContext recordContext;

    
    public void setup() {
        StreamsConfig streamsConfig = mock(StreamsConfig);
        expect(streamsConfig.getString(StreamsConfig.APPLICATION_ID_CONFIG)).andReturn("dummy-id");
        expect(streamsConfig.defaultValueSerde()).andReturn(Serdes.ByteArray());
        expect(streamsConfig.defaultKeySerde()).andReturn(Serdes.ByteArray());
        replay(streamsConfig);

        StateManager stateManager = mock(StateManager);
        expect(stateManager.getGlobalStore(GLOBAL_STORE_NAME)).andReturn(mock(StateStore));
        expect(stateManager.getGlobalStore(GLOBAL_KEY_VALUE_STORE_NAME)).andReturn(mock(KeyValueStore));
        expect(stateManager.getGlobalStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME)).andReturn(mock(TimestampedKeyValueStore));
        expect(stateManager.getGlobalStore(GLOBAL_WINDOW_STORE_NAME)).andReturn(mock(WindowStore));
        expect(stateManager.getGlobalStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME)).andReturn(mock(TimestampedWindowStore));
        expect(stateManager.getGlobalStore(GLOBAL_SESSION_STORE_NAME)).andReturn(mock(SessionStore));
        expect(stateManager.getGlobalStore(UNKNOWN_STORE)).andReturn(null);
        replay(stateManager);

        globalContext = new GlobalProcessorContextImpl(
            streamsConfig,
            stateManager,
            null,
            null);

        ProcessorNode processorNode = mock(ProcessorNode);
        globalContext.setCurrentNode(processorNode);

        child = mock(ProcessorNode);

        expect(processorNode.children())
            .andReturn(Collections.singletonList(child))
            .anyTimes();
        expect(processorNode.getChild(CHILD_PROCESSOR))
            .andReturn(child);
        expect(processorNode.getChild(anyString()))
            .andReturn(null);
        replay(processorNode);

        recordContext = mock(ProcessorRecordContext);
        globalContext.setRecordContext(recordContext);
    }

    [Xunit.Fact]
    public void shouldReturnGlobalOrNullStore() {
        Assert.Equal(globalContext.getStateStore(GLOBAL_STORE_NAME), new IsInstanceOf(StateStore));
        assertNull(globalContext.getStateStore(UNKNOWN_STORE));
    }

    
    [Xunit.Fact]
    public void shouldForwardToSingleChild() {
        child.process(null, null);
        expectLastCall();

        replay(child, recordContext);
        globalContext.forward(null, null);
        verify(child, recordContext);
    }

    [Xunit.Fact]// (expected = IllegalStateException)
    public void shouldFailToForwardUsingToParameter() {
        globalContext.forward(null, null, To.all());
    }

     // need to test deprecated code until removed
    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void shouldNotSupportForwardingViaChildIndex() {
        globalContext.forward(null, null, 0);
    }

     // need to test deprecated code until removed
    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void shouldNotSupportForwardingViaChildName() {
        globalContext.forward(null, null, "processorName");
    }

    [Xunit.Fact]
    public void shouldNotFailOnNoOpCommit() {
        globalContext.commit();
    }

    
    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void shouldNotAllowToSchedulePunctuationsUsingDeprecatedApi() {
        globalContext.schedule(0L, null, null);
    }

    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void shouldNotAllowToSchedulePunctuations() {
        globalContext.schedule(null, null, null);
    }

    [Xunit.Fact]
    public void shouldNotAllowInitForKeyValueStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_KEY_VALUE_STORE_NAME);
        try {
            store.init(null, null);
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void shouldNotAllowInitForTimestampedKeyValueStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME);
        try {
            store.init(null, null);
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void shouldNotAllowInitForWindowStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_WINDOW_STORE_NAME);
        try {
            store.init(null, null);
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void shouldNotAllowInitForTimestampedWindowStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME);
        try {
            store.init(null, null);
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void shouldNotAllowInitForSessionStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_SESSION_STORE_NAME);
        try {
            store.init(null, null);
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void shouldNotAllowCloseForKeyValueStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_KEY_VALUE_STORE_NAME);
        try {
            store.close();
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void shouldNotAllowCloseForTimestampedKeyValueStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME);
        try {
            store.close();
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void shouldNotAllowCloseForWindowStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_WINDOW_STORE_NAME);
        try {
            store.close();
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void shouldNotAllowCloseForTimestampedWindowStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME);
        try {
            store.close();
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void shouldNotAllowCloseForSessionStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_SESSION_STORE_NAME);
        try {
            store.close();
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }
}