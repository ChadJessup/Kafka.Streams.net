using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Serialization;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;

namespace Kafka.Streams.State.ChangeLogging
{
    public class ChangeLoggingSessionBytesStore
        : WrappedStateStore<ISessionStore<Bytes, byte[]>, Bytes, byte[]>,
        ISessionStore<Bytes, byte[]>
    {
        private StoreChangeLogger<Bytes, byte[]> changeLogger;

        public ChangeLoggingSessionBytesStore(
            KafkaStreamsContext context,
            ISessionStore<Bytes, byte[]> bytesStore)
            : base(context, bytesStore)
        {
        }

        public override void Init(IProcessorContext context, IStateStore root)
        {
            base.Init(context, root);
            string topic = ProcessorStateManager.StoreChangelogTopic(
                    context.ApplicationId,
                    Name);

            changeLogger = new StoreChangeLogger<Bytes, byte[]>(
                    Name,
                    context,
                    new StateSerdes<Bytes, byte[]>(
                        topic, 
                        new BytesSerdes(), 
                        Serdes.ByteArray()));
        }


        public IKeyValueIterator<Windowed<Bytes>, byte[]> FindSessions(
            Bytes key,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            return Wrapped.FindSessions(
                key,
                earliestSessionEndTime,
                latestSessionStartTime);
        }

        public IKeyValueIterator<Windowed<Bytes>, byte[]> FindSessions(
            Bytes keyFrom,
            Bytes keyTo,
            long earliestSessionEndTime,
            long latestSessionStartTime)
        {
            return Wrapped.FindSessions(
                keyFrom,
                keyTo,
                earliestSessionEndTime,
                latestSessionStartTime);
        }

        public void Remove(Windowed<Bytes> sessionKey)
        {
            Wrapped.Remove(sessionKey);
            changeLogger.LogChange(SessionKeySchema.ToBinary(sessionKey), null);
        }

        public void Put(Windowed<Bytes> sessionKey, byte[] aggregate)
        {
            Wrapped.Put(sessionKey, aggregate);
            changeLogger.LogChange(SessionKeySchema.ToBinary(sessionKey), aggregate);

        }

        public byte[] FetchSession(Bytes key, long startTime, long endTime)
        {
            return Wrapped.FetchSession(key, startTime, endTime);
        }

        public IKeyValueIterator<Windowed<Bytes>, byte[]> Fetch(Bytes key)
        {
            return Wrapped.Fetch(key);
        }

        public IKeyValueIterator<Windowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to)
        {
            return Wrapped.Fetch(from, to);
        }
    }
}
