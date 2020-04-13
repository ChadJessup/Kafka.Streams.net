using System;
using Kafka.Common.Utils;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.TimeStamped;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.State.Internals
{
    public class InMemoryTimestampedWindowStoreMarker : IWindowStore<Bytes, byte[]>, ITimestampedBytesStore
    {
        private IWindowStore<Bytes, byte[]> wrapped;

        public InMemoryTimestampedWindowStoreMarker(IWindowStore<Bytes, byte[]> wrapped)
        {
            this.wrapped = wrapped ?? throw new ArgumentNullException(nameof(wrapped));

            if (wrapped.Persistent())
            {
                throw new ArgumentException("Provided store must not be a Persistent store, but it is.");
            }
        }

        public void Init(IProcessorContext context, IStateStore root)
        {
            this.wrapped.Init(context, root);
        }

        public void Put(Bytes key, byte[] value)
        {
            this.wrapped.Add(key, value);
        }


        public void Put(Bytes key, byte[] value, DateTime windowStartTimestamp)
        {
            this.wrapped.Put(key, value, windowStartTimestamp);
        }

        public byte[] Fetch(Bytes key, DateTime time)
        {
            return this.wrapped.Fetch(key, time);
        }

        public IWindowStoreIterator<byte[]> Fetch(Bytes key, DateTime timeFrom, DateTime timeTo)
        {
            return this.wrapped.Fetch(key, timeFrom, timeTo);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(
            Bytes from,
            Bytes to,
            DateTime timeFrom,
            DateTime timeTo)
        {
            return this.wrapped.Fetch(from, to, timeFrom, timeTo);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FetchAll(DateTime timeFrom, DateTime timeTo)
        {
            return this.wrapped.FetchAll(timeFrom, timeTo);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> All()
        {
            return this.wrapped.All();
        }

        public void Flush()
        {
            this.wrapped.Flush();
        }

        public void Close()
        {
            this.wrapped.Close();
        }

        public bool IsOpen()
        {
            return this.wrapped.IsOpen();
        }

        public string Name => this.wrapped.Name;

        public bool Persistent()
        {
            return false;
        }

        public void Add(Bytes key, byte[] value)
        {
            this.wrapped.Add(key, value);
        }

        public bool IsPresent()
        {
            return this.wrapped.IsPresent();
        }
    }
}
