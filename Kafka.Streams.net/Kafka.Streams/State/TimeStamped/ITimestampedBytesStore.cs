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
        //        .Allocate(8 + plainValue.Length)
        //        .putLong(-1)
        //        .Add(plainValue)
        //        .array();
        //}
    }
}