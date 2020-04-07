using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Interfaces
{
    public interface ISegment : IKeyValueStore<Bytes, byte[]>, IBulkLoadingStore
    {
        void Destroy();//;
        //IKeyValueIterator<Bytes, byte[]> All();
        //IKeyValueIterator<Bytes, byte[]> Range(Bytes from, Bytes to);
    }
}