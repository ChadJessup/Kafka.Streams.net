//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Internals;
//using Moq;
//using Xunit;

//namespace Kafka.Streams.Tests.Kstream.Internals
//{
//    public class TimestampedTupleForwarderTest
//    {

//        [Fact]
//        public void ShouldSetFlushListenerOnWrappedStateStore()
//        {
//            SetFlushListener(true);
//            SetFlushListener(false);
//        }

//        private void SetFlushListener(bool sendOldValues)
//        {
//            WrappedStateStore<IStateStore, object, IValueAndTimestamp<object>> store = Mock.Of<WrappedStateStore<IStateStore, object, IValueAndTimestamp<object>>>();
//            FlushListener<object, object> flushListener = Mock.Of<FlushListener<object, object>>();

//            expect(store.SetFlushListener(flushListener, sendOldValues)).andReturn(false);
//            replay(store);

//            new TimestampedTupleForwarder<>(store, null, flushListener, sendOldValues);

//            verify(store);
//        }

//        [Fact]
//        public void ShouldForwardRecordsIfWrappedStateStoreDoesNotCache()
//        {
//            ShouldForwardRecordsIfWrappedStateStoreDoesNotCache(false);
//            ShouldForwardRecordsIfWrappedStateStoreDoesNotCache(true);
//        }

//        private void ShouldForwardRecordsIfWrappedStateStoreDoesNotCache(bool sendOldValues)
//        {
//            WrappedStateStore<IStateStore, string, string> store = Mock.Of<WrappedStateStore<IStateStore, string, string>>();
//            IProcessorContext context = Mock.Of<IProcessorContext>();

//            expect(store.SetFlushListener(null, sendOldValues)).andReturn(false);
//            if (sendOldValues)
//            {
//                context.Forward("key1", new Change<string>("newValue1", "oldValue1"));
//                context.Forward("key2", new Change<string>("newValue2", "oldValue2"), To.All().WithTimestamp(42L));
//            }
//            else
//            {
//                context.Forward("key1", new Change<string>("newValue1", null));
//                context.Forward("key2", new Change<string>("newValue2", null), To.All().WithTimestamp(42L));
//            }

//            expect.AstCall();
//            replay(store, context);

//            TimestampedTupleForwarder<string, string> forwarder =
//                new TimestampedTupleForwarder<string, string>(store, context, null, sendOldValues);
//            forwarder.MaybeForward("key1", "newValue1", "oldValue1");
//            forwarder.MaybeForward("key2", "newValue2", "oldValue2", 42L);

//            verify(store, context);
//        }

//        [Fact]
//        public void ShouldNotForwardRecordsIfWrappedStateStoreDoesCache()
//        {
//            WrappedStateStore<IStateStore, string, string> store = Mock.Of<WrappedStateStore<IStateStore, string, string>>();
//            IProcessorContext context = Mock.Of<IProcessorContext>();

//            expect(store.SetFlushListener(null, false)).andReturn(true);
//            replay(store, context);

//            TimestampedTupleForwarder<string, string> forwarder =
//                new TimestampedTupleForwarder<string, string>(store, context, null, false);
//            forwarder.MaybeForward("key", "newValue", "oldValue");
//            forwarder.MaybeForward("key", "newValue", "oldValue", 42L);

//            verify(store, context);
//        }
//    }
//}
