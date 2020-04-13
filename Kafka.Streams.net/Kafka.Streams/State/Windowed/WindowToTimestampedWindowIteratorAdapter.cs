using System;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.State.Windowed
{
    public class WindowToTimestampedWindowIteratorAdapter
    : KeyValueToTimestampedKeyValueIteratorAdapter<DateTime>,
        IWindowStoreIterator<byte[]>
    {
        public WindowToTimestampedWindowIteratorAdapter(IKeyValueIterator<DateTime, byte[]> innerIterator)
            : base(innerIterator)
        {
        }
    }
}
