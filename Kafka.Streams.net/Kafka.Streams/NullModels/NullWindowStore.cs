using System;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.NullModels
{
    public class NullWindowStore : IWindowStore<Bytes, byte[]>
    {
        public string Name { get; private set; } = nameof(NullWindowStore);

        public void Add(Bytes key, byte[] value)
        {
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> All()
            => new NullKeyValueIterator<IWindowed<Bytes>, byte[]>();

        public void Close()
        {
        }

        public IWindowStoreIterator<byte[]> Fetch(Bytes key, DateTime timeFrom, DateTime timeTo)
            => new NullKeyValueIterator<byte[]>();

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to, DateTime timeFrom, DateTime timeTo)
            => new NullKeyValueIterator<IWindowed<Bytes>, byte[]>();

        public byte[] Fetch(Bytes key, DateTime time) => Array.Empty<byte>();

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FetchAll(DateTime timeFrom, DateTime timeTo)
            => new NullKeyValueIterator<IWindowed<Bytes>, byte[]>();

        public void Flush()
        {
        }

        public void Init(IProcessorContext context, IStateStore root)
        {
        }

        public bool IsOpen() => true;

        public bool IsPresent() => true;

        public bool Persistent() => true;

        public void Put(Bytes key, byte[] value)
        {
        }

        public void Put(Bytes key, byte[] value, DateTime windowStartTimestamp)
        {
        }
    }
}
