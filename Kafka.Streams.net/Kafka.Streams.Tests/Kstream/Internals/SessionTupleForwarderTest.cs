//using Kafka.Streams.Processors;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State;
//using Kafka.Streams.State.Internals;

//namespace Kafka.Streams.KStream.Internals
//{
//    public class SessionTupleForwarderTest
//    {

//        [Fact]
//        public void shouldSetFlushListenerOnWrappedStateStore()
//        {
//            setFlushListener(true);
//            setFlushListener(false);
//        }

//        private void setFlushListener(bool sendOldValues)
//        {
//            WrappedStateStore<StateStore, Windowed<object>, object> store = mock(WrappedStateStore));
//            SessionCacheFlushListener<object, object> flushListener = mock(SessionCacheFlushListener));

//            expect(store.setFlushListener(flushListener, sendOldValues)).andReturn(false);
//            replay(store);

//            new SessionTupleForwarder<>(store, null, flushListener, sendOldValues);

//            verify(store);
//        }

//        [Fact]
//        public void shouldForwardRecordsIfWrappedStateStoreDoesNotCache()
//        {
//            shouldForwardRecordsIfWrappedStateStoreDoesNotCache(false);
//            shouldForwardRecordsIfWrappedStateStoreDoesNotCache(true);
//        }

//        private void shouldForwardRecordsIfWrappedStateStoreDoesNotCache(bool sendOldValued)
//        {
//            WrappedStateStore<StateStore, string, string> store = mock(WrappedStateStore));
//            IProcessorContext context = mock(IProcessorContext));

//            expect(store.setFlushListener(null, sendOldValued)).andReturn(false);
//            if (sendOldValued)
//            {
//                context.forward(
//                    new Windowed<>("key", new SessionWindow(21L, 42L)),
//                    new Change<>("value", "oldValue"),
//                    To.All().WithTimestamp(42L));
//            }
//            else
//            {
//                context.forward(
//                    new Windowed<>("key", new SessionWindow(21L, 42L)),
//                    new Change<>("value", null),
//                    To.All().WithTimestamp(42L));
//            }
//            expect.AstCall();
//            replay(store, context);

//            new SessionTupleForwarder<>(store, context, null, sendOldValued)
//                .maybeForward(new Windowed<>("key", new SessionWindow(21L, 42L)), "value", "oldValue");

//            verify(store, context);
//        }

//        [Fact]
//        public void shouldNotForwardRecordsIfWrappedStateStoreDoesCache()
//        {
//            WrappedStateStore<IStateStore, string, string> store = mock(WrappedStateStore));
//            IProcessorContext context = mock(IProcessorContext));

//            expect(store.setFlushListener(null, false)).andReturn(true);
//            replay(store, context);

//            new SessionTupleForwarder<>(store, context, null, false)
//                .maybeForward(new Windowed<>("key", new SessionWindow(21L, 42L)), "value", "oldValue");

//            verify(store, context);
//        }

//    }
