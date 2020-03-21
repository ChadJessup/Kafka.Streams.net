using Kafka.Streams.State.KeyValues;
using System;

namespace Kafka.Streams.State.Window
{
    /**
     * IEnumerator interface of {@link KeyValuePair} with key typed {@link long} used for {@link WindowStore#fetch(object, long, long)}
     * and {@link WindowStore#fetch(object, Instant, Instant)}
     *
     * Users must call its {@code close} method explicitly upon completeness to release resources,
     * or use try-with-resources statement (available since JDK7) for this {@link IDisposable}.
     *
     * @param Type of values
     */
    public interface IWindowStoreIterator<V> : IKeyValueIterator<long, V>, IDisposable
    {
    }
}