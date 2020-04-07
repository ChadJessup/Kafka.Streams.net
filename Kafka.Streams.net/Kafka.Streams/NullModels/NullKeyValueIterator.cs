using System;
using System.Collections;
using System.Collections.Generic;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.NullModels
{
    internal class NullKeyValueIterator : IKeyValueIterator<Bytes, byte[]>
    {
        public KeyValuePair<Bytes, byte[]> Current { get; }
        object IEnumerator.Current { get; }

        public void Close()
        {
        }

        public void Dispose()
        {
        }

        public bool MoveNext() => false;

        public Bytes PeekNextKey() => new Bytes(Array.Empty<byte>());

        public void Reset()
        {
        }
    }
}
