﻿using Kafka.Streams.KStream;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.Processors.Internals
{
    public class SessionStoreReadWriteDecorator<K, AGG>
        : StateStoreReadWriteDecorator<ISessionStore<K, AGG>>,
        ISessionStore<K, AGG>
    {
        public SessionStoreReadWriteDecorator(ISessionStore<K, AGG> inner)
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
            wrapped.Remove(sessionKey);
        }

        public void Put(Windowed<K> sessionKey, AGG aggregate)
        {
            wrapped.Put(sessionKey, aggregate);
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
