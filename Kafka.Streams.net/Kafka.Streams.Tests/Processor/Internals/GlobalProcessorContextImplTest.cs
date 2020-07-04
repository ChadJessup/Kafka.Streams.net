//using Kafka.Streams.Configs;
//using Kafka.Streams.KStream;
//using Kafka.Streams.Nodes;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Internals;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Interfaces;
//using Kafka.Streams.State.KeyValues;
//using Kafka.Streams.State.Sessions;
//using Kafka.Streams.State.TimeStamped;
//using Kafka.Streams.State.Windowed;
//using Moq;
//using System;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class GlobalProcessorContextImplTest
//    {
//        private const string GLOBAL_STORE_NAME = "global-store";
//        private const string GLOBAL_KEY_VALUE_STORE_NAME = "global-key-value-store";
//        private const string GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME = "global-timestamped-key-value-store";
//        private const string GLOBAL_WINDOW_STORE_NAME = "global-window-store";
//        private const string GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME = "global-timestamped-window-store";
//        private const string GLOBAL_SESSION_STORE_NAME = "global-session-store";
//        private const string UNKNOWN_STORE = "unknown-store";
//        private const string CHILD_PROCESSOR = "child";

//        private GlobalProcessorContext globalContext;

//        private IProcessorNode child;
//        private ProcessorRecordContext recordContext;


//        public void Setup()
//        {
//            StreamsConfig streamsConfig = Mock.Of<StreamsConfig>();
//            expect(streamsConfig.getString(StreamsConfig.ApplicationIdConfig)).andReturn("dummy-id");
//            expect(streamsConfig.GetDefaultValueSerde()).andReturn(Serdes.ByteArray());
//            expect(streamsConfig.GetDefaultKeySerde()).andReturn(Serdes.ByteArray());
//            replay(streamsConfig);

//            IStateManager stateManager = Mock.Of<IStateManager>();
//            expect(stateManager.GetGlobalStore(GLOBAL_STORE_NAME)).andReturn(Mock.Of<IStateStore>());
//            expect(stateManager.GetGlobalStore(GLOBAL_KEY_VALUE_STORE_NAME)).andReturn(Mock.Of<IKeyValueStore>());
//            expect(stateManager.GetGlobalStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME)).andReturn(Mock.Of<ITimestampedKeyValueStore>());
//            expect(stateManager.GetGlobalStore(GLOBAL_WINDOW_STORE_NAME)).andReturn(Mock.Of<IWindowStore>());
//            expect(stateManager.GetGlobalStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME)).andReturn(Mock.Of<ITimestampedWindowStore>());
//            expect(stateManager.GetGlobalStore(GLOBAL_SESSION_STORE_NAME)).andReturn(Mock.Of<ISessionStore>());
//            expect(stateManager.GetGlobalStore(UNKNOWN_STORE)).andReturn(null);
//            replay(stateManager);

//            globalContext = new GlobalProcessorContext(
//                streamsConfig,
//                stateManager,
//                null,
//                null);

//            ProcessorNode processorNode = Mock.Of<ProcessorNode>();
//            globalContext.setCurrentNode(processorNode);

//            child = Mock.Of<ProcessorNode>();

//            expect(processorNode.children)
//                .andReturn(Collections.singletonList(child))
//                .anyTimes();
//            expect(processorNode.GetChild(CHILD_PROCESSOR))
//                .andReturn(child);
//            expect(processorNode.GetChild(string.Empty))
//                .andReturn(null);
//            replay(processorNode);

//            recordContext = Mock.Of<ProcessorRecordContext>();
//            globalContext.setRecordContext(recordContext);
//        }

//        [Fact]
//        public void ShouldReturnGlobalOrNullStore()
//        {
//            Assert.Equal(globalContext.getStateStore(GLOBAL_STORE_NAME), new IsInstanceOf(IStateStore));
//            Assert.Null(globalContext.getStateStore(UNKNOWN_STORE));
//        }


//        [Fact]
//        public void ShouldForwardToSingleChild()
//        {
//            child.Process<string, string>(null, null);
//            expectLastCall();

//            replay(child, recordContext);
//            globalContext.Forward(null, null);
//            verify(child, recordContext);
//        }

//        [Fact]// (expected = IllegalStateException)
//        public void ShouldFailToForwardUsingToParameter()
//        {
//            globalContext.Forward(null, null, To.All());
//        }

//        // need to test deprecated code until removed
//        [Fact]// (expected = NotImplementedException)
//        public void ShouldNotSupportForwardingViaChildIndex()
//        {
//            globalContext.Forward(null, null, 0);
//        }

//        // need to test deprecated code until removed
//        [Fact]// (expected = NotImplementedException)
//        public void ShouldNotSupportForwardingViaChildName()
//        {
//            globalContext.Forward(null, null, "processorName");
//        }

//        [Fact]
//        public void ShouldNotFailOnNoOpCommit()
//        {
//            globalContext.Commit();
//        }


//        [Fact]// (expected = NotImplementedException)
//        public void ShouldNotAllowToSchedulePunctuationsUsingDeprecatedApi()
//        {
//            globalContext.schedule(0L, null, null);
//        }

//        [Fact]// (expected = NotImplementedException)
//        public void ShouldNotAllowToSchedulePunctuations()
//        {
//            globalContext.schedule(null, null, null);
//        }

//        [Fact]
//        public void ShouldNotAllowInitForKeyValueStore()
//        {
//            IStateStore store = globalContext.getStateStore(GLOBAL_KEY_VALUE_STORE_NAME);
//            try
//            {
//                store.Init(null, null);
//                Assert.True(false, "Should have thrown NotImplementedException.");
//            }
//            catch (NotImplementedException expected) { }
//        }

//        [Fact]
//        public void ShouldNotAllowInitForTimestampedKeyValueStore()
//        {
//            IStateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME);
//            try
//            {
//                store.Init(null, null);
//                Assert.True(false, "Should have thrown NotImplementedException.");
//            }
//            catch (NotImplementedException expected) { }
//        }

//        [Fact]
//        public void ShouldNotAllowInitForWindowStore()
//        {
//            IStateStore store = globalContext.getStateStore(GLOBAL_WINDOW_STORE_NAME);
//            try
//            {
//                store.Init(null, null);
//                Assert.True(false, "Should have thrown NotImplementedException.");
//            }
//            catch (NotImplementedException expected) { }
//        }

//        [Fact]
//        public void ShouldNotAllowInitForTimestampedWindowStore()
//        {
//            IStateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME);
//            try
//            {
//                store.Init(null, null);
//                Assert.True(false, "Should have thrown NotImplementedException.");
//            }
//            catch (NotImplementedException expected) { }
//        }

//        [Fact]
//        public void ShouldNotAllowInitForSessionStore()
//        {
//            IStateStore store = globalContext.getStateStore(GLOBAL_SESSION_STORE_NAME);
//            try
//            {
//                store.Init(null, null);
//                Assert.True(false, "Should have thrown NotImplementedException.");
//            }
//            catch (NotImplementedException expected) { }
//        }

//        [Fact]
//        public void ShouldNotAllowCloseForKeyValueStore()
//        {
//            IStateStore store = globalContext.getStateStore(GLOBAL_KEY_VALUE_STORE_NAME);
//            try
//            {
//                store.Close();
//                Assert.True(false, "Should have thrown NotImplementedException.");
//            }
//            catch (NotImplementedException expected) { }
//        }

//        [Fact]
//        public void ShouldNotAllowCloseForTimestampedKeyValueStore()
//        {
//            IStateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_KEY_VALUE_STORE_NAME);
//            try
//            {
//                store.Close();
//                Assert.True(false, "Should have thrown NotImplementedException.");
//            }
//            catch (NotImplementedException expected) { }
//        }

//        [Fact]
//        public void ShouldNotAllowCloseForWindowStore()
//        {
//            IStateStore store = globalContext.getStateStore(GLOBAL_WINDOW_STORE_NAME);
//            try
//            {
//                store.Close();
//                Assert.True(false, "Should have thrown NotImplementedException.");
//            }
//            catch (NotImplementedException expected) { }
//        }

//        [Fact]
//        public void ShouldNotAllowCloseForTimestampedWindowStore()
//        {
//            IStateStore store = globalContext.getStateStore(GLOBAL_TIMESTAMPED_WINDOW_STORE_NAME);
//            try
//            {
//                store.Close();
//                Assert.True(false, "Should have thrown NotImplementedException.");
//            }
//            catch (NotImplementedException expected) { }
//        }

//        [Fact]
//        public void ShouldNotAllowCloseForSessionStore()
//        {
//            IStateStore store = globalContext.getStateStore(GLOBAL_SESSION_STORE_NAME);
//            try
//            {
//                store.Close();
//                Assert.True(false, "Should have thrown NotImplementedException.");
//            }
//            catch (NotImplementedException expected) { }
//        }
//    }
//}
