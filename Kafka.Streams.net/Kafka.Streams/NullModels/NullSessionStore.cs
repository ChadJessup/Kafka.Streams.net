using System;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;

namespace Kafka.Streams.NullModels
{
    internal class NullSessionStore : ISessionStore<Bytes, byte[]>
    {
        public string Name { get; }

        public void Close()
        {
        }

        public IKeyValueIterator<Windowed<Bytes>, byte[]> Fetch(Bytes key)
            => new NullKeyValueIterator<Windowed<Bytes>, byte[]>();

        public IKeyValueIterator<Windowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to)
            => new NullKeyValueIterator<Windowed<Bytes>, byte[]>();

        public byte[] FetchSession(Bytes key, long startTime, long endTime)
            => Array.Empty<byte>();

        public IKeyValueIterator<Windowed<Bytes>, byte[]> FindSessions(Bytes key, long earliestSessionEndTime, long latestSessionStartTime)
            => new NullKeyValueIterator<Windowed<Bytes>, byte[]>();

        public IKeyValueIterator<Windowed<Bytes>, byte[]> FindSessions(Bytes keyFrom, Bytes keyTo, long earliestSessionEndTime, long latestSessionStartTime)
            => new NullKeyValueIterator<Windowed<Bytes>, byte[]>();

        public void Flush()
        {
        }

        public void Init(IProcessorContext context, IStateStore root)
        {
        }

        public bool IsOpen() => true;
        public bool IsPresent() => true;
        public bool Persistent() => true;

        public void Put(Windowed<Bytes> sessionKey, byte[] aggregate)
        {
        }

        public void Remove(Windowed<Bytes> sessionKey)
        {
        }
    }
}
