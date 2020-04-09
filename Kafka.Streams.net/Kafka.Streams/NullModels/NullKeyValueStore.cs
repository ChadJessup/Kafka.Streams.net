using System;
using System.Collections.Generic;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.KeyValues;

namespace Kafka.Streams.NullModels
{
    internal class NullKeyValueStore : IKeyValueStore<Bytes, byte[]>
    {
        private byte[] EmptyBytes => Array.Empty<byte>();

        public string Name { get; }
        public long approximateNumEntries { get; }

        public void Add(Bytes key, byte[] value)
        {
        }

        public IKeyValueIterator<Bytes, byte[]> All()
        {
            return new NullKeyValueIterator<Bytes, byte[]>();
        }

        public void Close()
        {
        }

        public byte[] Delete(Bytes key) => this.EmptyBytes;

        public void Flush()
        {
        }

        public byte[] Get(Bytes key) => this.EmptyBytes;

        public void Init(IProcessorContext context, IStateStore root)
        {
        }

        public bool IsOpen() => true;

        public bool IsPresent() => true;

        public bool Persistent() => true;

        public void PutAll(List<KeyValuePair<Bytes, byte[]>> entries)
        {
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value) => this.EmptyBytes;

        public IKeyValueIterator<Bytes, byte[]> Range(Bytes from, Bytes to)
            => new NullKeyValueIterator<Bytes, byte[]>();
    }
}
