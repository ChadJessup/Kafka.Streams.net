using Kafka.Streams.KStream;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public class SessionStoreReadOnlyDecorator<K, AGG>
        : StateStoreReadOnlyDecorator<ISessionStore<K, AGG>>
        , ISessionStore<K, AGG>
    {
        public SessionStoreReadOnlyDecorator(ISessionStore<K, AGG> inner)
            : base(inner)
        {
        }

        public IKeyValueIterator<Windowed<K>, AGG> FindSessions(
            K key,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            return wrapped.FindSessions(key, earliestSessionEndTime, latestSessionStartTime);
        }

        public IKeyValueIterator<Windowed<K>, AGG> FindSessions(
            K keyFrom,
            K keyTo,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            return wrapped.FindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        }

        public void Remove(Windowed<K> sessionKey)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public void Put(Windowed<K> sessionKey,
                        AGG aggregate)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public AGG FetchSession(K key, long startTime, long endTime)
        {
            return wrapped.FetchSession(key, startTime, endTime);
        }

        public IKeyValueIterator<Windowed<K>, AGG> Fetch(K key)
        {
            return wrapped.Fetch(key);
        }

        public IKeyValueIterator<Windowed<K>, AGG> Fetch(K from, K to)
        {
            return wrapped.Fetch(from, to);
        }
    }
}

