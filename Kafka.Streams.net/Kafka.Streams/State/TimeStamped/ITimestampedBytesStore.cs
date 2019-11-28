
using Kafka.Streams.KStream.Internals;

namespace Kafka.Streams.State.TimeStamped
{
    public interface ITimestampedBytesStore
    {
        //byte[] convertToTimestampedFormat(byte[] plainValue);
        //{
        //    if (plainValue == null)
        //    {
        //        return null;
        //    }

        //    return new ByteBuffer()
        //        .allocate(8 + plainValue.Length)
        //        .putLong(-1)
        //        .Add(plainValue)
        //        .array();
        //}
    }
}