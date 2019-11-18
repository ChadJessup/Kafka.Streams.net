
//using Kafka.Common.Utils;

//namespace Kafka.Streams.State.Internals
//{
//    public class ChangeLoggingTimestampedKeyValueBytesStore : ChangeLoggingKeyValueBytesStore
//    {

//        public ChangeLoggingTimestampedKeyValueBytesStore(IKeyValueStore<Bytes, byte[]> inner)
//            : base(inner)
//        {
//        }


//        void log(Bytes key,
//                 byte[] valueAndTimestamp)
//        {
//            if (valueAndTimestamp != null)
//            {
//                changeLogger.logChange(key, rawValue(valueAndTimestamp), timestamp(valueAndTimestamp));
//            }
//            else
//            {
//                changeLogger.logChange(key, null);
//            }
//        }
//    }
//}