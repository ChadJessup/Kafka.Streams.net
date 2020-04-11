using System.Collections;
using System.Collections.Generic;

namespace Kafka.Streams.State.KeyValues
{
    public class KeyValueIteratorFacade<K, V> : IKeyValueIterator<K, V>
    {
        private readonly IKeyValueIterator<K, V> iterator;

        public KeyValuePair<K, V> Current { get; }
        object IEnumerator.Current { get; }

        public KeyValueIteratorFacade(IKeyValueIterator<K, V> iterator)
        {
            this.iterator = iterator;
        }

        public bool HasNext()
        {
            return this.iterator.MoveNext();
        }

        public K PeekNextKey()
        {
            return this.iterator.PeekNextKey();
        }

        public KeyValuePair<K, V> Next()
        {
            var innerKeyValue = this.iterator.Current;

            var vat = (IValueAndTimestamp<V>)(object)innerKeyValue.Value;
            return KeyValuePair.Create(innerKeyValue.Key, ValueAndTimestamp.GetValueOrNull(vat));
        }

        public void Close()
        {
            this.iterator.Close();
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
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                this.disposedValue = true;
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
            this.Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
