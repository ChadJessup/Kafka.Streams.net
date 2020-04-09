using Kafka.Streams.KStream;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.KeyValues;

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

        public IKeyValueIterator<Windowed<K>, AGG> FindSessions(
            K key,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            return Wrapped.FindSessions(key, earliestSessionEndTime, latestSessionStartTime);
        }

        public IKeyValueIterator<Windowed<K>, AGG> FindSessions(
            K keyFrom,
            K keyTo,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            return Wrapped.FindSessions(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime);
        }

        public void Remove(Windowed<K> sessionKey)
        {
            Wrapped.Remove(sessionKey);
        }

        public void Put(Windowed<K> sessionKey, AGG aggregate)
        {
            Wrapped.Put(sessionKey, aggregate);
        }

        public AGG FetchSession(K key, long startTime, long endTime)
        {
            return Wrapped.FetchSession(key, startTime, endTime);
        }

        public IKeyValueIterator<Windowed<K>, AGG> Fetch(K key)
        {
            return Wrapped.Fetch(key);
        }

        public IKeyValueIterator<Windowed<K>, AGG> Fetch(K from, K to)
        {
            return Wrapped.Fetch(from, to);
        }
    }
}
