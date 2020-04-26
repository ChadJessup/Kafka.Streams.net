using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.KeyValues;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class CompositeKeyValueIterator<K, V, StoreType> : IKeyValueIterator<K, V>
    {
        private readonly IEnumerator<StoreType> storeIterator;
        private readonly INextIteratorFunction<K, V, StoreType> nextIteratorFunction;

        private IKeyValueIterator<K, V> current;

        public KeyValuePair<K, V> Current { get; }
        object IEnumerator.Current { get; }

        public CompositeKeyValueIterator(
            IEnumerator<StoreType> underlying,
            INextIteratorFunction<K, V, StoreType> nextIteratorFunction)
        {
            this.storeIterator = underlying;
            this.nextIteratorFunction = nextIteratorFunction;
        }

        public void Close()
        {
            if (this.current != null)
            {
                this.current.Close();
                this.current = null;
            }
        }

        public K PeekNextKey()
        {
            throw new InvalidOperationException("peekNextKey not supported");
        }

        public bool HasNext()
        {
            while ((this.current == null || !this.current.MoveNext())
                    && this.storeIterator.MoveNext())
            {
                this.Close();
                this.current = this.nextIteratorFunction.Apply(this.storeIterator.Current);
            }

            return this.current != null && this.current.MoveNext();
        }


        public KeyValuePair<K, V> Next()
        {
            if (!this.MoveNext())
            {
                throw new IndexOutOfRangeException();
            }

            return this.current.Current;
        }

        public void Remove()
        {
            throw new InvalidOperationException("Remove not supported");
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
        // ~CompositeKeyValueIterator()
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