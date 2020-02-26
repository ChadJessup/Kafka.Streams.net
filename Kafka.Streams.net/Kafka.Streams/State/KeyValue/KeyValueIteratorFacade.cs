using System.Collections;
using Kafka.Streams.KStream;
using Kafka.Streams.State.Interfaces;

namespace Kafka.Streams.State.KeyValue
{
    public class KeyValueIteratorFacade<K, V> : IKeyValueIterator<K, V>
    {
        private readonly IKeyValueIterator<K, V> iterator;

        public KeyValue<K, V> Current { get; }
        object IEnumerator.Current { get; }

        public KeyValueIteratorFacade(IKeyValueIterator<K, V> iterator)
        {
            this.iterator = iterator;
        }

        public bool hasNext()
        {
            return iterator.MoveNext();
        }

        public K peekNextKey()
        {
            return iterator.peekNextKey();
        }

        public KeyValue<K, V> next()
        {
            var innerKeyValue = iterator.Current;

            var vat = (ValueAndTimestamp<V>)(object)innerKeyValue.Value;
            return KeyValue<K, V>.Pair(innerKeyValue.Key, ValueAndTimestamp<V>.GetValueOrNull(vat));
        }

        public void close()
        {
            iterator.close();
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
        private readonly IKeyValueIterator<Windowed<K>, ValueAndTimestamp<V>> innerIterator1;

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