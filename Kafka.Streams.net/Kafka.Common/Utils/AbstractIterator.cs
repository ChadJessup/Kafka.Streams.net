/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

        public bool hasNext()
        {
            return state switch
            {
                State.FAILED => throw new InvalidOperationException("Iterator is in failed state"),
                State.DONE => false,
                State.READY => true,
                _ => maybeComputeNext(),
            };
        }

        public T next()
        {
            if (!hasNext())
                throw new KeyNotFoundException();
            state = State.NOT_READY;
            if (_next == null)
                throw new InvalidOperationException("Expected item but none found.");
            return _next;
        }

        public void remove()
        {
            throw new InvalidOperationException("Removal not supported");
        }

        public T peek()
        {
            if (!hasNext())
                throw new KeyNotFoundException();
            return _next;
        }

        protected T allDone()
        {
            state = State.DONE;
            return default;
        }

        protected abstract T makeNext();

        private bool maybeComputeNext()
        {
            state = State.FAILED;
            _next = makeNext();
            if (state == State.DONE)
            {
                return false;
            }
            else
            {
                state = State.READY;
                return true;
            }
        }

        public bool MoveNext()
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
        // ~AbstractIterator()
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
    }
}
