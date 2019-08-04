using Kafka.Common.Utils;

namespace Kafka.Streams.State.Internals
{
    public interface IEldestEntryRemovalListener
    {
        void apply(Bytes key, byte[] value);
    }
}