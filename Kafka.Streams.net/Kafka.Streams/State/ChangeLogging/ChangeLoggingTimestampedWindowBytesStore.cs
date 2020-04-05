
//namespace Kafka.Streams.State.Internals
//{

//    class ChangeLoggingTimestampedWindowBytesStore : ChangeLoggingWindowBytesStore
//    {

//        ChangeLoggingTimestampedWindowBytesStore(IWindowStore<Bytes, byte[]> bytesStore,
//                                                 bool retainDuplicates)
//            : base(bytesStore, retainDuplicates)
//        {
//        }


//        void log(Bytes key,
//                 byte[] valueAndTimestamp)
//        {
//            if (valueAndTimestamp != null)
//            {
//                changeLogger.logChange(key, RawValue(valueAndTimestamp), timestamp(valueAndTimestamp));
//            }
//            else
//            {
//                changeLogger.logChange(key, null);
//            }
//        }
//    }
//}