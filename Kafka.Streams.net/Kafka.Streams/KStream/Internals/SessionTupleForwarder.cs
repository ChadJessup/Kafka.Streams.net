
//    public class SessionTupleForwarder<K, V>
//    {
//        private IProcessorContext context;
//        private bool sendOldValues;
//        private bool cachingEnabled;


//        SessionTupleForwarder(IStateStore store,
//                               IProcessorContext context,
//                               ICacheFlushListener<IWindowed<K>, V> flushListener,
//                               bool sendOldValues)
//        {
//            this.context = context;
//            this.sendOldValues = sendOldValues;
//            cachingEnabled = ((WrappedStateStore)store).setFlushListener(flushListener, sendOldValues);
//        }

//        public void maybeForward(IWindowed<K> key,
//                                  V newValue,
//                                  V oldValue)
//        {
//            if (!cachingEnabled)
//            {
//                context.Forward(key, new Change<>(newValue, sendOldValues ? oldValue : null), To.All().WithTimestamp(key.window.end()));
//            }
//        }
//    }
//}
