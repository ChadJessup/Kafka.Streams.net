using Kafka.Streams.State.Interfaces;
using System;
using Kafka.Streams.KStream;

namespace Kafka.Streams.Processors.Internals
{
    public class SessionStoreReadWriteDecorator<K, AGG>
        : StateStoreReadWriteDecorator<ISessionStore<K, AGG>>
        , ISessionStore<K, AGG>
    {
        public SessionStoreReadWriteDecorator(ISessionStore<K, AGG> inner)
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
            wrapped.Remove(sessionKey);
        }

        public void put(Windowed<K> sessionKey, AGG aggregate)
        {
            wrapped.put(sessionKey, aggregate);
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
