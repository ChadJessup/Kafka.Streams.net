using Kafka.Common.Utils;
using Kafka.Streams.State.Internals;

namespace Kafka.Streams.State.Interfaces
{
    public interface ISegment : IKeyValueStore<Bytes, byte[]>, IBulkLoadingStore
    {
        void destroy();//;

        KeyValueIterator<Bytes, byte[]> all();

        KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to);
    }
}