using Kafka.Streams.KStream;
using Kafka.Streams.State.Interfaces;
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

        public IKeyValueIterator<Windowed<K>, AGG> findSessions(
            K key,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            return wrapped.findSessions(key, earliestSessionEndTime, latestSessionStartTime);
        }

        public IKeyValueIterator<Windowed<K>, AGG> findSessions(
            K keyFrom,
            K keyTo,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            return wrapped.findSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        }

        public void Remove(Windowed<K> sessionKey)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public void put(Windowed<K> sessionKey,
                        AGG aggregate)
        {
            throw new InvalidOperationException(ERROR_MESSAGE);
        }

        public AGG fetchSession(K key, long startTime, long endTime)
        {
            return wrapped.fetchSession(key, startTime, endTime);
        }

        public IKeyValueIterator<Windowed<K>, AGG> fetch(K key)
        {
            return wrapped.fetch(key);
        }

        public IKeyValueIterator<Windowed<K>, AGG> fetch(K from, K to)
        {
            return wrapped.fetch(from, to);
        }
    }
}

