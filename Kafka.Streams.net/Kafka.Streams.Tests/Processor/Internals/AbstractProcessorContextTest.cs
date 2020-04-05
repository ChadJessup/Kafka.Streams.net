//using Confluent.Kafka;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Internals;
//using System;
//using Xunit;

//namespace Kafka.Streams.Tests.Processor.Internals
//{
//    public class AbstractProcessorContextTest
//    {
//        //private readonly MockStreamsMetrics metrics = new MockStreamsMetrics(new Metrics());
//        private AbstractProcessorContext context = new TestProcessorContext();
//        private readonly MockKeyValueStore stateStore = new MockKeyValueStore("store", false);
//        private Headers headers = new Headers(new Header[] { new Headers("key", "value".getBytes()) });
//        private ProcessorRecordContext recordContext = new ProcessorRecordContext(10, System.currentTimeMillis(), 1, "foo", headers);


//        public void Before()
//        {
//            context.SetRecordContext(recordContext);
//        }

//        [Xunit.Fact]
//        public void ShouldThrowIllegalStateExceptionOnRegisterWhenContextIsInitialized()
//        {
//            context.Initialize();
//            try
//            {
//                context.register(stateStore, null);
//                Assert.True(false, "should throw illegal state exception when context already initialized");
//            }
//            catch (IllegalStateException e)
//            {
//                // pass
//            }
//        }

//        [Xunit.Fact]
//        public void ShouldNotThrowIllegalStateExceptionOnRegisterWhenContextIsNotInitialized()
//        {
//            context.register(stateStore, null);
//        }

//        [Xunit.Fact]// (expected = NullPointerException)
//        public void ShouldThrowNullPointerOnRegisterIfStateStoreIsNull()
//        {
//            context.register(null, null);
//        }

//        [Xunit.Fact]
//        public void ShouldThrowIllegalStateExceptionOnTopicIfNoRecordContext()
//        {
//            context.SetRecordContext(null);
//            try
//            {
//                context.Topic;
//                Assert.True(false, "should throw illegal state exception when record context is null");
//            }
//            catch (IllegalStateException e)
//            {
//                // pass
//            }
//        }

//        [Xunit.Fact]
//        public void ShouldReturnTopicFromRecordContext()
//        {
//            Assert.Equal(context.Topic, (recordContext.Topic));
//        }

//        [Xunit.Fact]
//        public void ShouldReturnNullIfTopicEqualsNonExistTopic()
//        {
//            context.SetRecordContext(new ProcessorRecordContext(0, 0, 0, AbstractProcessorContext.NONEXIST_TOPIC, null));
//            Assert.Equal(context.Topic, nullValue());
//        }

//        [Xunit.Fact]
//        public void ShouldThrowIllegalStateExceptionOnPartitionIfNoRecordContext()
//        {
//            context.SetRecordContext(null);
//            try
//            {
//                context.Partition;
//                Assert.True(false, "should throw illegal state exception when record context is null");
//            }
//            catch (IllegalStateException e)
//            {
//                // pass
//            }
//        }

//        [Xunit.Fact]
//        public void ShouldReturnPartitionFromRecordContext()
//        {
//            Assert.Equal(context.Partition, (recordContext.Partition));
//        }

//        [Xunit.Fact]
//        public void ShouldThrowIllegalStateExceptionOnOffsetIfNoRecordContext()
//        {
//            context.SetRecordContext(null);
//            try
//            {
//                context.Offset;
//            }
//            catch (IllegalStateException e)
//            {
//                // pass
//            }
//        }

//        [Xunit.Fact]
//        public void ShouldReturnOffsetFromRecordContext()
//        {
//            Assert.Equal(context.Offset, (recordContext.Offset));
//        }

//        [Xunit.Fact]
//        public void ShouldThrowIllegalStateExceptionOnTimestampIfNoRecordContext()
//        {
//            context.SetRecordContext(null);
//            try
//            {
//                context.Timestamp;
//                Assert.True(false, "should throw illegal state exception when record context is null");
//            }
//            catch (IllegalStateException e)
//            {
//                // pass
//            }
//        }

//        [Xunit.Fact]
//        public void ShouldReturnTimestampFromRecordContext()
//        {
//            Assert.Equal(context.Timestamp, (recordContext.Timestamp));
//        }

//        [Xunit.Fact]
//        public void ShouldReturnHeadersFromRecordContext()
//        {
//            Assert.Equal(context.Headers, (recordContext.Headers));
//        }

//        [Xunit.Fact]
//        public void ShouldReturnNullIfHeadersAreNotSet()
//        {
//            context.SetRecordContext(new ProcessorRecordContext(0, 0, 0, AbstractProcessorContext.NONEXIST_TOPIC, null));
//            Assert.Equal(context.Headers, nullValue());
//        }

//        [Xunit.Fact]
//        public void ShouldThrowIllegalStateExceptionOnHeadersIfNoRecordContext()
//        {
//            context.SetRecordContext(null);
//            try
//            {
//                context.Headers;
//            }
//            catch (IllegalStateException e)
//            {
//                // pass
//            }
//        }


//        [Xunit.Fact]
//        public void AppConfigsShouldReturnParsedValues()
//        {
//            Assert.Equal(
//                context.AppConfigs().Get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG),
//                equalTo(RocksDBConfigSetter));
//        }

//        [Xunit.Fact]
//        public void AppConfigsShouldReturnUnrecognizedValues()
//        {
//            Assert.Equal(
//                context.AppConfigs().Get("user.supplied.config"),
//                equalTo("user-suppplied-value"));
//        }


//        private static class TestProcessorContext : AbstractProcessorContext
//        {
//            static StreamsConfig config;
//            //static {
//            config = getStreamsConfig();
//            // Value must be a string to test className => class conversion
//            config.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, RocksDBConfigSetter.getName());
//            config.put("user.supplied.config", "user-suppplied-value");
//        //}

//        TestProcessorContext(MockStreamsMetrics metrics)
//            : base(new TaskId(0, 0), new StreamsConfig(config), metrics, new StateManagerStub(), new ThreadCache(new LogContext("name "), 0, metrics))
//            {
//            }


//            public IStateStore GetStateStore(string name)
//            {
//                return null;
//            }


//            [Obsolete]
//            public Cancellable Schedule(long interval,
//    PunctuationType type,
//    Punctuator callback)
//            {
//                return null;
//            }


//            public Cancellable Schedule(Duration interval,
//                                        PunctuationType type,
//                                        Punctuator callback)
//            {// throws ArgumentException
//                return null;
//            }


//            public void Forward<K, V>(K key, V value) { }


//            public void Forward<K, V>(K key, V value, To to) { }


//            [Obsolete]
//            public void Forward<K, V>(K key, V value, int childIndex) { }


//            [Obsolete]
//            public void Forward<K, V>(K key, V value, string childName) { }


//            public void Commit() { }
//        }
//    }
//}