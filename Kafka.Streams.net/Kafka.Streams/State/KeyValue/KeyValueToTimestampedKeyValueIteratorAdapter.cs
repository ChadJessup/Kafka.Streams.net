using System.Collections;
using System.Collections.Generic;

using static Kafka.Streams.State.Internals.ValueAndTimestampDeserializer;

namespace Kafka.Streams.State.KeyValues
{
    public class KeyValueToTimestampedKeyValueIteratorAdapter<K> : IKeyValueIterator<K, byte[]>
    {
        private IKeyValueIterator<K, byte[]> innerIterator;

        public KeyValueToTimestampedKeyValueIteratorAdapter(IKeyValueIterator<K, byte[]> innerIterator)
        {
            this.innerIterator = innerIterator;
        }

        public void Close()
        {
            innerIterator.Close();
        }

        public K PeekNextKey()
        {
            return innerIterator.PeekNextKey();
        }

        public bool MoveNext()
        {
            return innerIterator.MoveNext();
        }

        public KeyValuePair<K, byte[]> Current
        {
            get
            {
                KeyValuePair<K, byte[]> plainKeyValue = innerIterator.Current;
                return KeyValuePair.Create(plainKeyValue.Key, ConvertToTimestampedFormat(plainKeyValue.Value));
            }
        }

        object IEnumerator.Current => this.Current;

        public void Reset()
        {
        }

        public void Dispose()
        {
        }
    }
}
