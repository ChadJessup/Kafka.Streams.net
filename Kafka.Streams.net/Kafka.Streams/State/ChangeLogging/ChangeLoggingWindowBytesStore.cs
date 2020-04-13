using System;
using Kafka.Streams.KStream;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.Internals;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.State.ChangeLogging
{
    public class ChangeLoggingWindowBytesStore
        : WrappedStateStore<IWindowStore<Bytes, byte[]>, byte[], byte[]>,
        IWindowStore<Bytes, byte[]>
    {
        private bool retainDuplicates;
        private IProcessorContext context;
        private int seqnum = 0;

        private StoreChangeLogger<Bytes, byte[]> changeLogger;

        public ChangeLoggingWindowBytesStore(
            KafkaStreamsContext context,
            IWindowStore<Bytes, byte[]> bytesStore,
            bool retainDuplicates)
                : base(context, bytesStore)
        {
            this.retainDuplicates = retainDuplicates;
        }

        public override void Init(IProcessorContext context, IStateStore root)
        {
            this.context = context;
            base.Init(context, root);
            string topic = ProcessorStateManager.StoreChangelogTopic(context.ApplicationId, this.Name);

            this.changeLogger = new StoreChangeLogger<Bytes, byte[]>(
                this.Name,
                context,
                new StateSerdes<Bytes, byte[]>(topic, Serdes.Bytes(), Serdes.ByteArray()));
        }

        public byte[] Fetch(Bytes key, DateTime timestamp)
        {
            return this.Wrapped.Fetch(key, timestamp);
        }

        public IWindowStoreIterator<byte[]> Fetch(Bytes key, DateTime from, DateTime to)
        {
            return this.Wrapped.Fetch(key, from, to);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(
            Bytes keyFrom,
            Bytes keyTo,
            DateTime from,
            DateTime to)
        {
            return this.Wrapped.Fetch(keyFrom, keyTo, from, to);
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> All()
        {
            return this.Wrapped.All();
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FetchAll(
            DateTime timeFrom,
            DateTime timeTo)
        {
            return this.Wrapped.FetchAll(timeFrom, timeTo);
        }

        public void Put(Bytes key, byte[] value)
        {
            // Note: It's incorrect to bypass the wrapped store here by delegating to another method,
            // but we have no alternative. We must send a timestamped key to the changelog, which means
            // we need to know what timestamp gets used for the record. Hopefully, we can deprecate this
            // method in the future to resolve the situation.
            this.Put(key, value, this.context.Timestamp);
        }

        public void Put(Bytes key, byte[] value, DateTime windowStartTimestamp)
        {
            this.Wrapped.Put(key, value, windowStartTimestamp);
            this.log(WindowKeySchema.ToStoreKeyBinary(
                key,
                windowStartTimestamp,
                this.MaybeUpdateSeqnumForDups()),
                value);
        }

        void log(Bytes key, byte[] value)
        {
            this.changeLogger.LogChange(key, value);
        }

        private int MaybeUpdateSeqnumForDups()
        {
            if (this.retainDuplicates)
            {
                this.seqnum = (this.seqnum + 1) & 0x7FFFFFFF;
            }

            return this.seqnum;
        }

        public void Add(Bytes key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public byte[] Fetch(Bytes key, long time)
        {
            throw new NotImplementedException();
        }

        public IWindowStoreIterator<byte[]> Fetch(Bytes key, long timeFrom, long timeTo)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> Fetch(Bytes from, Bytes to, long timeFrom, long timeTo)
        {
            throw new NotImplementedException();
        }

        public IKeyValueIterator<IWindowed<Bytes>, byte[]> FetchAll(long timeFrom, long timeTo)
        {
            throw new NotImplementedException();
        }
    }
}
