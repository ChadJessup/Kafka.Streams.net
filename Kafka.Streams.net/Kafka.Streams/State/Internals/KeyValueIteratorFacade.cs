using System.Collections;
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.State.Internals
{
    public class KeyValueIteratorFacade<K, V> : IKeyValueIterator<K, V>
    {
        private readonly IKeyValueIterator<K, ValueAndTimestamp<V>> innerIterator;

        public KeyValue<K, V> Current { get; }
        object IEnumerator.Current { get; }

        public KeyValueIteratorFacade(IKeyValueIterator<K, ValueAndTimestamp<V>> iterator)
        {
            innerIterator = iterator;
        }

        public bool hasNext()
        {
            return innerIterator.MoveNext();
        }

        public K peekNextKey()
        {
            return innerIterator.peekNextKey();
        }

        public KeyValue<K, V> next()
        {
            KeyValue<K, ValueAndTimestamp<V>> innerKeyValue = innerIterator.Current;

            return KeyValue<K, V>.Pair(innerKeyValue.Key, ValueAndTimestamp<V>.getValueOrNull(innerKeyValue.Value));
        }

        public void close()
        {
            innerIterator.close();
        }

        public bool MoveNext()
        {
            throw new System.NotImplementedException();
        }

        public void Reset()
        {
            throw new System.NotImplementedException();
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~KeyValueIteratorFacade()
        // {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}