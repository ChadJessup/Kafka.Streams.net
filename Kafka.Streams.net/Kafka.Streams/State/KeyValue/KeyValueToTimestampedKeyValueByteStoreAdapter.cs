using System;
using System.Collections.Generic;
using Kafka.Streams.KStream.Internals;
using Kafka.Streams.Processors.Interfaces;

using static Kafka.Streams.State.Internals.ValueAndTimestampDeserializer;

namespace Kafka.Streams.State.KeyValues
{
    public class KeyValueToTimestampedKeyValueByteStoreAdapter : IKeyValueStore<Bytes, byte[]>
    {
        private readonly IKeyValueStore<Bytes, byte[]> store;

        public string Name => this.store.Name;
        public long approximateNumEntries => this.store.approximateNumEntries;

        public void Add(Bytes key, byte[] value) => this.store.Add(key, value);
        public bool IsPresent() => this.store.IsPresent();

        public KeyValueToTimestampedKeyValueByteStoreAdapter(IKeyValueStore<Bytes, byte[]> store)
        {
            if (!store.Persistent())
            {
                throw new ArgumentException("Provided store must be a Persistent store, but it is not.");
            }

            this.store = store;
        }

        public void Put(Bytes key, byte[] valueWithTimestamp)
        {
            this.store.Add(key, valueWithTimestamp == null
                ? null
                : RawValue(valueWithTimestamp));
        }

        public byte[] PutIfAbsent(Bytes key, byte[] valueWithTimestamp)
        {
            return ConvertToTimestampedFormat(this.store.PutIfAbsent(
                key,
                valueWithTimestamp == null
                ? null
                : RawValue(valueWithTimestamp)));
        }

        public void PutAll(List<KeyValuePair<Bytes, byte[]>> entries)
        {
            foreach (KeyValuePair<Bytes, byte[]> entry in entries)
            {
                byte[] valueWithTimestamp = entry.Value;
                this.store.Add(entry.Key, valueWithTimestamp == null ? null : RawValue(valueWithTimestamp));
            }
        }

        public byte[] Delete(Bytes key)
        {
            return ConvertToTimestampedFormat(this.store.Delete(key));
        }

        public void Init(IProcessorContext context, IStateStore root)
        {
            this.store.Init(context, root);
        }

        public void Flush()
        {
            this.store.Flush();
        }

        public void Close() => this.store.Close();
        public bool Persistent() => true;
        public bool IsOpen() => this.store.IsOpen();

        public byte[] Get(Bytes key)
        {
            return ConvertToTimestampedFormat(this.store.Get(key)) ?? Array.Empty<byte>();
        }

        public IKeyValueIterator<Bytes, byte[]> Range(Bytes from, Bytes to)
        {
            return new KeyValueToTimestampedKeyValueIteratorAdapter<Bytes>(this.store.Range(from, to));
        }

        public IKeyValueIterator<Bytes, byte[]> All()
        {
            return new KeyValueToTimestampedKeyValueIteratorAdapter<Bytes>(this.store.All());
        }
    }
}
