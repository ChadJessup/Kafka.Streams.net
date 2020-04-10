using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Windowed
{
    public class WindowToTimestampedWindowIteratorAdapter
    : KeyValueToTimestampedKeyValueIteratorAdapter<long>,
        IWindowStoreIterator<byte[]>
    {
        public WindowToTimestampedWindowIteratorAdapter(IKeyValueIterator<long, byte[]> innerIterator)
            : base(innerIterator)
        {
        }
    }
}
