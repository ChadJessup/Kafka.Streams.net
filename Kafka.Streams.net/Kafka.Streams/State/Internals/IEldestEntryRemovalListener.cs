namespace Kafka.Streams.State.Internals
{
    public interface IEldestEntryRemovalListener
    {
        void Apply(Bytes key, byte[] value);
    }
}