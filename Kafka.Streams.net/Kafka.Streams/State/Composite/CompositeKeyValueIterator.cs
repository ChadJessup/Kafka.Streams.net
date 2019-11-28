using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.KeyValue;
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

        public KeyValue<K, V> Current { get; }
        object IEnumerator.Current { get; }

        public CompositeKeyValueIterator(
            IEnumerator<StoreType> underlying,
            INextIteratorFunction<K, V, StoreType> nextIteratorFunction)
        {
            this.storeIterator = underlying;
            this.nextIteratorFunction = nextIteratorFunction;
        }

        public void close()
        {
            if (current != null)
            {
                current.close();
                current = null;
            }
        }

        public K peekNextKey()
        {
            throw new InvalidOperationException("peekNextKey not supported");
        }

        public bool hasNext()
        {
            while ((current == null || !current.MoveNext())
                    && storeIterator.MoveNext())
            {
                close();
                current = nextIteratorFunction.apply(storeIterator.Current);
            }

            return current != null && current.MoveNext();
        }


        public KeyValue<K, V> next()
        {
            if (!hasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return current.Current;
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
        // ~CompositeKeyValueIterator()
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