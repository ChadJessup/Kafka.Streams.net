using Kafka.Streams.KStream;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.KeyValues;
using System;

namespace Kafka.Streams.Processors.Internals
{
    public class SessionStoreReadWriteDecorator<K, AGG>
        : StateStoreReadWriteDecorator<ISessionStore<K, AGG>, K, AGG>,
        ISessionStore<K, AGG>
    {
        public SessionStoreReadWriteDecorator(
            KafkaStreamsContext context,
            ISessionStore<K, AGG> inner)
            : base(context, inner)
        {
        }

        public IKeyValueIterator<IWindowed<K>, AGG> FindSessions(
            K key,
            DateTime earliestSessionEndTime,
            DateTime latestSessionStartTime)
        {
            return this.Wrapped.FindSessions(
                key,
                earliestSessionEndTime,
                latestSessionStartTime);
        }

        public IKeyValueIterator<IWindowed<K>, AGG> FindSessions(
            K keyFrom,
            K keyTo,
            DateTime earliestSessionEndTime,
            DateTime latestSessionStartTime)
        {
            return this.Wrapped.FindSessions(
                keyFrom,
                keyTo,
                earliestSessionEndTime,
                latestSessionStartTime);
        }

        public void Remove(IWindowed<K> sessionKey)
        {
            this.Wrapped.Remove(sessionKey);
        }

        public void Put(IWindowed<K> sessionKey, AGG aggregate)
        {
            this.Wrapped.Put(sessionKey, aggregate);
        }

        public AGG FetchSession(K key, DateTime startTime, DateTime endTime)
        {
            return this.Wrapped.FetchSession(key, startTime, endTime);
        }

        public IKeyValueIterator<IWindowed<K>, AGG> Fetch(K key)
        {
            return this.Wrapped.Fetch(key);
        }

        public IKeyValueIterator<IWindowed<K>, AGG> Fetch(K from, K to)
        {
            return this.Wrapped.Fetch(from, to);
        }
    }
}
