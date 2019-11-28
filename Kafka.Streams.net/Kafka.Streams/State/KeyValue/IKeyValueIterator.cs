using Kafka.Streams.KStream;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.KeyValue
{
    /**
     * IEnumerator interface of {@link KeyValue}.
     *
     * Users must call its {@code close} method explicitly upon completeness to release resources,
     * or use try-with-resources statement (available since JDK7) for this {@link IDisposable}.
     *
     * @param Type of keys
     * @param Type of values
     */
    public interface IKeyValueIterator<K, V> : IEnumerator<KeyValue<K, V>>, IDisposable
    {
        abstract void close();

        /**
         * Peek at the next key without advancing the iterator
         * @return the key of the next value that would be returned from the next call to next
         */
        K peekNextKey();
    }
}