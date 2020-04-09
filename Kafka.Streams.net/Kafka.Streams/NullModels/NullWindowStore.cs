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

        public IKeyValueIterator<Windowed<Bytes>, byte[]> All()
            => new NullKeyValueIterator<Windowed<Bytes>, byte[]>();

        public void Close()
        {
        }

        public IWindowStoreIterator<byte[]> Fetch(Bytes key, long timeFrom, long timeTo)
            => new NullKeyValueIterator<byte[]>();

        public IKeyValueIterator<Windowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to, long timeFrom, long timeTo)
            => new NullKeyValueIterator<Windowed<Bytes>, byte[]>();

        public byte[] Fetch(Bytes key, long time) => Array.Empty<byte>();

        public IWindowStoreIterator<byte[]> Fetch(Bytes key, DateTime from, DateTime to)
            => new NullKeyValueIterator<byte[]>();

        public IKeyValueIterator<Windowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to, DateTime fromTime, DateTime toTime)
            => new NullKeyValueIterator<Windowed<Bytes>, byte[]>();

        public IKeyValueIterator<Windowed<Bytes>, byte[]> FetchAll(long timeFrom, long timeTo)
            => new NullKeyValueIterator<Windowed<Bytes>, byte[]>();

        public IKeyValueIterator<Windowed<Bytes>, byte[]> FetchAll(DateTime from, DateTime to)
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

        public void Put(Bytes key, byte[] value)
        {
        }

        public void Put(Bytes key, byte[] value, long windowStartTimestamp)
        {
        }
    }
}
