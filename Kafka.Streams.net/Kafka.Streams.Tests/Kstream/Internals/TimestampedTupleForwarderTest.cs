///*
//
//
//
//
//
//
// *
//
// *
//
//
//
//
//
// */
//using Kafka.Streams.KStream.Internals;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Internals;

//namespace Kafka.Streams.KStream.Internals
//{














//    public class TimestampedTupleForwarderTest
//    {

//        [Fact]
//        public void shouldSetFlushListenerOnWrappedStateStore()
//        {
//            setFlushListener(true);
//            setFlushListener(false);
//        }

//        private void setFlushListener(bool sendOldValues)
//        {
//            WrappedStateStore<StateStore, object, ValueAndTimestamp<object>> store = mock(WrappedStateStore));
//            TimestampedCacheFlushListener<object, object> flushListener = mock(TimestampedCacheFlushListener));

//            expect(store.setFlushListener(flushListener, sendOldValues)).andReturn(false);
//            replay(store);

//            new TimestampedTupleForwarder<>(store, null, flushListener, sendOldValues);

//            verify(store);
//        }

//        [Fact]
//        public void shouldForwardRecordsIfWrappedStateStoreDoesNotCache()
//        {
//            shouldForwardRecordsIfWrappedStateStoreDoesNotCache(false);
//            shouldForwardRecordsIfWrappedStateStoreDoesNotCache(true);
//        }

//        private void shouldForwardRecordsIfWrappedStateStoreDoesNotCache(bool sendOldValues)
//        {
//            WrappedStateStore<StateStore, string, string> store = mock(WrappedStateStore));
//            IProcessorContext context = mock(IProcessorContext));

//            expect(store.setFlushListener(null, sendOldValues)).andReturn(false);
//            if (sendOldValues)
//            {
//                context.forward("key1", new Change<>("newValue1", "oldValue1"));
//                context.forward("key2", new Change<>("newValue2", "oldValue2"), To.All().WithTimestamp(42L));
//            }
//            else
//            {
//                context.forward("key1", new Change<>("newValue1", null));
//                context.forward("key2", new Change<>("newValue2", null), To.All().WithTimestamp(42L));
//            }
//            expect.AstCall();
//            replay(store, context);

//            TimestampedTupleForwarder<string, string> forwarder =
//                new TimestampedTupleForwarder<>(store, context, null, sendOldValues);
//            forwarder.maybeForward("key1", "newValue1", "oldValue1");
//            forwarder.maybeForward("key2", "newValue2", "oldValue2", 42L);

//            verify(store, context);
//        }

//        [Fact]
//        public void shouldNotForwardRecordsIfWrappedStateStoreDoesCache()
//        {
//            WrappedStateStore<IStateStore, string, string> store = mock(WrappedStateStore));
//            IProcessorContext context = mock(IProcessorContext));

//            expect(store.setFlushListener(null, false)).andReturn(true);
//            replay(store, context);

//            TimestampedTupleForwarder<string, string> forwarder =
//                new TimestampedTupleForwarder<>(store, context, null, false);
//            forwarder.maybeForward("key", "newValue", "oldValue");
//            forwarder.maybeForward("key", "newValue", "oldValue", 42L);

//            verify(store, context);
//        }
//    }
//}
