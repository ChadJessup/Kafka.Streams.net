///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements. See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
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
