using Kafka.Streams.State.KeyValues;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class EmptyKeyValueIterator<K, V> : IKeyValueIterator<K, V>
    {
        public KeyValuePair<K, V> Current { get; }
        object IEnumerator.Current { get; }

        public void Close()
        {
        }

        public K PeekNextKey()
        {
            throw new IndexOutOfRangeException();
        }


        public bool HasNext()
        {
            return false;
        }


        public KeyValuePair<K, V> Next()
        {
            throw new IndexOutOfRangeException();
        }


        public void Remove()
        {
        }

        public bool MoveNext()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
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
        // ~EmptyKeyValueIterator()
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
