using System;
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
                    this.Name);

            this.changeLogger = new StoreChangeLogger<Bytes, byte[]>(
                    this.Name,
                    context,
                    new StateSerdes<Bytes, byte[]>(
                        topic, 
                        new BytesSerdes(), 
                        Serdes.ByteArray()));
        }


        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FindSessions(
            Bytes key,
            DateTime earliestSessionEndTime,
            DateTime latestSessionStartTime)
        {
            return this.Wrapped.FindSessions(
                key,
                earliestSessionEndTime,
                latestSessionStartTime);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FindSessions(
            Bytes keyFrom,
            Bytes keyTo,
            DateTime earliestSessionEndTime,
            DateTime latestSessionStartTime)
        {
            return this.Wrapped.FindSessions(
                keyFrom,
                keyTo,
                earliestSessionEndTime,
                latestSessionStartTime);
        }

        public void Remove(IWindowed<Bytes> sessionKey)
        {
            this.Wrapped.Remove(sessionKey);
            this.changeLogger.LogChange(SessionKeySchema.ToBinary(sessionKey), null);
        }

        public void Put(IWindowed<Bytes> sessionKey, byte[] aggregate)
        {
            this.Wrapped.Put(sessionKey, aggregate);
            this.changeLogger.LogChange(SessionKeySchema.ToBinary(sessionKey), aggregate);

        }

        public byte[] FetchSession(Bytes key, DateTime startTime, DateTime endTime)
        {
            return this.Wrapped.FetchSession(key, startTime, endTime);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes key)
        {
            return this.Wrapped.Fetch(key);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to)
        {
            return this.Wrapped.Fetch(from, to);
        }
    }
}
