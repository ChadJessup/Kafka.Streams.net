using System;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.KeyValues;

using static Kafka.Streams.State.Internals.ValueAndTimestampDeserializer;

namespace Kafka.Streams.State.Windowed {
    public class WindowToTimestampedWindowByteStoreAdapter : IWindowStore<Bytes, byte[]>
    {
        private readonly IWindowStore<Bytes, byte[]> store;

        public WindowToTimestampedWindowByteStoreAdapter(IWindowStore<Bytes, byte[]> store)
        {
            this.store = store ?? throw new ArgumentNullException(nameof(store));

            if (!store.Persistent())
            {
                throw new ArgumentException("Provided store must be a Persistent store, but it is not.");
            }
        }

        public void Put(Bytes key, byte[] valueWithTimestamp)
        {
            this.store.Add(key, valueWithTimestamp == null
                ? null
                : RawValue(valueWithTimestamp));
        }

        public void Put(Bytes key, byte[] valueWithTimestamp, DateTime windowStartTimestamp)
        {
            this.store.Put(key, valueWithTimestamp == null
                ? null
                : RawValue(valueWithTimestamp), windowStartTimestamp);
        }

        public byte[] Fetch(Bytes key, DateTime time)
        {
            return ConvertToTimestampedFormat(this.store.Fetch(key, time));
        }

        public IWindowStoreIterator<byte[]> Fetch(
            Bytes key,
            DateTime timeFrom,
            DateTime timeTo)
        {
            return new WindowToTimestampedWindowIteratorAdapter(
                this.store.Fetch(key, timeFrom, timeTo));
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(
            Bytes from,
            Bytes to,
            DateTime timeFrom,
            DateTime timeTo)
        {
            return new KeyValueToTimestampedKeyValueIteratorAdapter<IWindowed<Bytes>>(
                this.store.Fetch(from, to, timeFrom, timeTo));
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> All()
        {
            return new KeyValueToTimestampedKeyValueIteratorAdapter<IWindowed<Bytes>>(this.store.All());
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FetchAll(DateTime timeFrom, DateTime timeTo)
        {
            return new KeyValueToTimestampedKeyValueIteratorAdapter<IWindowed<Bytes>>(this.store.FetchAll(timeFrom, timeTo));
        }

        public string Name => this.store.Name;

        public void Init(IProcessorContext context, IStateStore root)
        {
            this.store.Init(context, root);
        }

        public void Flush()
        {
            this.store.Flush();
        }

        public void Close()
        {
            this.store.Close();
        }

        public bool Persistent()
        {
            return true;
        }

        public bool IsOpen()
        {
            return this.store.IsOpen();
        }

        public void Add(Bytes key, byte[] value) {
            throw new NotImplementedException();
        }

        public bool IsPresent() {
            throw new NotImplementedException();
        }
    }
}
