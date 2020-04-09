using System.Collections;
using System.Collections.Generic;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.NullModels
{
    internal class NullKeyValueIterator<T> : IWindowStoreIterator<byte[]>
    {
        public KeyValuePair<long, byte[]> Current { get; }
        object IEnumerator.Current { get; }

        public void Close()
        {
        }

        public void Dispose()
        {
        }

        public bool MoveNext() => true;

        public long PeekNextKey() => 0;

        public void Reset()
        {
        }
    }
}
