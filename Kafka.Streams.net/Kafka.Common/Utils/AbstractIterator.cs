using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Common.Utils
{
    /**
     * A base class that simplifies implementing an iterator
     * @param <T> The type of thing we are iterating over
     */
    public abstract class AbstractIterator<T> : IEnumerator<T>
    {
        private enum State
        {
            READY,
            NOT_READY,
            DONE,
            FAILED
        }

        private State state = State.NOT_READY;
        private T _next;

        public T Current { get; }
        object IEnumerator.Current { get; }

        public virtual bool HasNext()
        {
            return this.state switch
            {
                State.FAILED => throw new InvalidOperationException("Iterator is in failed state"),
                State.DONE => false,
                State.READY => true,
                _ => this.maybeComputeNext(),
            };
        }

        public virtual T next()
        {
            if (!this.HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            this.state = State.NOT_READY;
            if (this._next == null)
                throw new InvalidOperationException("Expected item but none found.");
            return this._next;
        }

        public virtual void remove()
        {
            throw new InvalidOperationException("Removal not supported");
        }

        public virtual T Peek()
        {
            if (!this.HasNext())
            {
                throw new IndexOutOfRangeException();
            }

            return this._next;
        }

        protected virtual T allDone()
        {
            this.state = State.DONE;
            return default;
        }

        public abstract T MakeNext();

        private bool maybeComputeNext()
        {
            this.state = State.FAILED;
            this._next = this.MakeNext();
            if (this.state == State.DONE)
            {
                return false;
            }
            else
            {
                this.state = State.READY;
                return true;
            }
        }

        public virtual bool MoveNext()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

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
        // ~AbstractIterator()
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
    }
}
