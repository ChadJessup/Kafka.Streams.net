using System;
using System.Collections.Generic;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;

namespace Kafka.Streams.State.Internals
{
    public class InMemoryTimestampedKeyValueStoreMarker : IKeyValueStore<Bytes, byte[]>, ITimestampedBytesStore
    {
        private readonly IKeyValueStore<Bytes, byte[]> wrapped;

        public InMemoryTimestampedKeyValueStoreMarker(IKeyValueStore<Bytes, byte[]> wrapped)
        {
            this.wrapped = wrapped ?? throw new ArgumentNullException(nameof(wrapped));

            if (wrapped.Persistent())
            {
                throw new ArgumentException("Provided store must not be a persistent store, but it is.");
            }
        }

        public void Init(IProcessorContext context, IStateStore root) => wrapped.Init(context, root);
        public void Add(Bytes key, byte[] value) => wrapped.Add(key, value);
        public byte[] PutIfAbsent(Bytes key, byte[] value) => wrapped.PutIfAbsent(key, value);
        public void PutAll(List<KeyValuePair<Bytes, byte[]>> entries) => wrapped.PutAll(entries);
        public byte[] Delete(Bytes key) => this.wrapped.Delete(key);
        public byte[] Get(Bytes key) => wrapped.Get(key);
        public IKeyValueIterator<Bytes, byte[]> Range(Bytes from, Bytes to) => wrapped.Range(from, to);
        public IKeyValueIterator<Bytes, byte[]> All() => wrapped.All();
        public long approximateNumEntries => wrapped.approximateNumEntries;
        public void Flush() => wrapped.Flush();
        public void Close() => wrapped.Close();
        public bool IsOpen() => wrapped.IsOpen();
        public string Name => wrapped.Name;
        public bool Persistent() => false;
        public bool IsPresent() => this.wrapped.IsPresent();
    }
}
