using System;
using System.Collections;
using System.Collections.Generic;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.NullModels
{
    internal class NullKeyValueIterator<T> : IWindowStoreIterator<byte[]>
    {
        public KeyValuePair<DateTime, byte[]> Current { get; }
        object IEnumerator.Current { get; }

        public void Close()
        {
        }

        public void Dispose()
        {
        }

        public bool MoveNext() => true;

        public DateTime PeekNextKey() => DateTime.MinValue;

        public void Reset()
        {
        }
    }
}
