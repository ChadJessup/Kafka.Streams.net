/*






 *

 *





 */



























public class GlobalProcessorContextImplTest {
    private static readonly string GLOBAL_STORE_NAME = "global-store";
    private static readonly string GLOBAL_KEY_VALUE_STORE_NAME = "global-key-value-store";
    private static readonly string GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME = "global-timestamped-key-value-store";
    private static readonly string GLOBAL_WINDOW_STORE_NAME = "global-window-store";
    private static readonly string GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME = "global-timestamped-window-store";
    private static readonly string GLOBAL_SESSION_STORE_NAME = "global-session-store";
    private static readonly string UNKNOWN_STORE = "unknown-store";
    private static readonly string CHILD_PROCESSOR = "child";

    private GlobalProcessorContextImpl globalContext;

    private ProcessorNode child;
    private ProcessorRecordContext recordContext;

    
    public void Setup() {
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
    public void ShouldReturnGlobalOrNullStore() {
        Assert.Equal(globalContext.getStateStore(GLOBAL_STORE_NAME), new IsInstanceOf(StateStore));
        assertNull(globalContext.getStateStore(UNKNOWN_STORE));
    }

    
    [Xunit.Fact]
    public void ShouldForwardToSingleChild() {
        child.process(null, null);
        expectLastCall();

        replay(child, recordContext);
        globalContext.forward(null, null);
        verify(child, recordContext);
    }

    [Xunit.Fact]// (expected = IllegalStateException)
    public void ShouldFailToForwardUsingToParameter() {
        globalContext.forward(null, null, To.all());
    }

     // need to test deprecated code until removed
    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void ShouldNotSupportForwardingViaChildIndex() {
        globalContext.forward(null, null, 0);
    }

     // need to test deprecated code until removed
    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void ShouldNotSupportForwardingViaChildName() {
        globalContext.forward(null, null, "processorName");
    }

    [Xunit.Fact]
    public void ShouldNotFailOnNoOpCommit() {
        globalContext.commit();
    }

    
    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void ShouldNotAllowToSchedulePunctuationsUsingDeprecatedApi() {
        globalContext.schedule(0L, null, null);
    }

    [Xunit.Fact]// (expected = UnsupportedOperationException)
    public void ShouldNotAllowToSchedulePunctuations() {
        globalContext.schedule(null, null, null);
    }

    [Xunit.Fact]
    public void ShouldNotAllowInitForKeyValueStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_KEY_VALUE_STORE_NAME);
        try {
            store.init(null, null);
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void ShouldNotAllowInitForTimestampedKeyValueStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME);
        try {
            store.init(null, null);
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void ShouldNotAllowInitForWindowStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_WINDOW_STORE_NAME);
        try {
            store.init(null, null);
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void ShouldNotAllowInitForTimestampedWindowStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME);
        try {
            store.init(null, null);
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void ShouldNotAllowInitForSessionStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_SESSION_STORE_NAME);
        try {
            store.init(null, null);
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void ShouldNotAllowCloseForKeyValueStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_KEY_VALUE_STORE_NAME);
        try {
            store.close();
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void ShouldNotAllowCloseForTimestampedKeyValueStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME);
        try {
            store.close();
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void ShouldNotAllowCloseForWindowStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_WINDOW_STORE_NAME);
        try {
            store.close();
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void ShouldNotAllowCloseForTimestampedWindowStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME);
        try {
            store.close();
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }

    [Xunit.Fact]
    public void ShouldNotAllowCloseForSessionStore() {
        StateStore store = globalContext.getStateStore(GLOBAL_SESSION_STORE_NAME);
        try {
            store.close();
            Assert.True(false, "Should have thrown UnsupportedOperationException.");
        } catch (UnsupportedOperationException expected) { }
    }
}