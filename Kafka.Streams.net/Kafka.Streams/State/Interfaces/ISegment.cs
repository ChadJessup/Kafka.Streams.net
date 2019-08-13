using Kafka.Common.Utils;
using Kafka.Streams.State.Internals;

namespace Kafka.Streams.State.Interfaces
{
    public interface ISegment //: IKeyValueStore<Bytes, byte[]>, IBulkLoadingStore
    {
        void destroy();//;

        IKeyValueIterator<Bytes, byte[]> all();

        IKeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to);
    }
}