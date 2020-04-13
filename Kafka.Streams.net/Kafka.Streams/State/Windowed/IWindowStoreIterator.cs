using Kafka.Streams.State.KeyValues;
using System;

namespace Kafka.Streams.State.Windowed
{
    /**
     * IEnumerator interface of {@link KeyValuePair} with key typed {@link long} used for {@link WindowStore#Fetch(object, long, long)}
     * and {@link WindowStore#Fetch(object, Instant, Instant)}
     *
     * Users must call its {@code Close} method explicitly upon completeness to release resources,
     * or use try-with-resources statement (available since JDK7) for this {@link IDisposable}.
     *
     * @param Type of values
     */
    public interface IWindowStoreIterator<V> : IKeyValueIterator<DateTime, V>, IDisposable
    {
    }
}
