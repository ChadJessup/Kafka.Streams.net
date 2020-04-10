using Kafka.Streams.Errors;
using Kafka.Streams.State.KeyValues;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Kafka.Streams.State.Internals
{
    /**
     * Optimized {@link KeyValueIterator} used when the same element could be peeked multiple times.
     */
    public class DelegatingPeekingKeyValueIterator<K, V> : IKeyValueIterator<K, V>, IPeekingKeyValueIterator<K, V>
    {
        private readonly IKeyValueIterator<K, V> underlying;
        private readonly string storeName;
        private KeyValuePair<K, V>? _next;

        private volatile bool open = true;

        KeyValuePair<K, V> IEnumerator<KeyValuePair<K, V>>.Current { get; }
        object IEnumerator.Current { get; }

        public DelegatingPeekingKeyValueIterator(
            string storeName,
            IKeyValueIterator<K, V> underlying)
        {
            this.storeName = storeName;
            this.underlying = underlying;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public K PeekNextKey()
        {
            if (!this.HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return this._next.Value.Key;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Close()
        {
            this.underlying.Close();
            this.open = false;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public bool HasNext()
        {
            if (!this.open)
            {
                throw new InvalidStateStoreException(string.Format("Store %s has closed", this.storeName));
            }

            if (this._next != null)
            {
                return true;
            }

            if (!this.underlying.MoveNext())
            {
                return false;
            }

            this._next = this.underlying.Current;
            return true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public KeyValuePair<K, V>? Next()
        {
            if (!this.HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            var result = this._next;
            this._next = null;

            return result;
        }

        public void Remove()
        {
            throw new InvalidOperationException("Remove() is not supported in " + this.GetType().FullName);
        }

        public KeyValuePair<K, V>? PeekNext()
        {
            if (!this.HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return this._next;
        }

        void IKeyValueIterator<K, V>.Close()
        {
            throw new System.NotImplementedException();
        }

        K IKeyValueIterator<K, V>.PeekNextKey()
        {
            throw new System.NotImplementedException();
        }

        bool IEnumerator.MoveNext()
        {
            throw new System.NotImplementedException();
        }

        void IEnumerator.Reset()
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
        // ~DelegatingPeekingKeyValueIterator()
        // {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        void System.IDisposable.Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            this.Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}