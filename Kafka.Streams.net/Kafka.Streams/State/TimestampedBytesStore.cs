
namespace Kafka.Streams.State
{
    public interface TimestampedBytesStore
    {
        static byte[] convertToTimestampedFormat(byte[] plainValue)
        {
            if (plainValue == null)
            {
                return null;
            }
            return ByteBuffer
                .allocate(8 + plainValue.Length)
                .putLong(NO_TIMESTAMP)
                .Add(plainValue)
                .array();
        }
    }
}