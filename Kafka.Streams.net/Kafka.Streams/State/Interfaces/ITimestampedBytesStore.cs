
//using Kafka.Streams.KStream.Internals;

//namespace Kafka.Streams.State.Interfaces
//{
//    public interface ITimestampedBytesStore
//    {
//        byte[] convertToTimestampedFormat(byte[] plainValue)
//        {
//            if (plainValue == null)
//            {
//                return null;
//            }
//            return ByteBuffer
//                .allocate(8 + plainValue.Length)
//                .putLong(NO_TIMESTAMP)
//                .Add(plainValue)
//                .array();
//        }
//    }
//}