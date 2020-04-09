using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.KeyValues;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class ChangeLoggingKeyValueBytesStore
        : WrappedStateStore<IKeyValueStore<Bytes, byte[]>, Bytes, byte[]>, IKeyValueStore<Bytes, byte[]>
    {
        public StoreChangeLogger<Bytes, byte[]> changeLogger { get; }

        public ChangeLoggingKeyValueBytesStore(KafkaStreamsContext context, IKeyValueStore<Bytes, byte[]> inner)
            : base(context, inner)
        {
        }

        public override void Init(
            IProcessorContext context,
            IStateStore root)
        {
            base.Init(context, root);
            var topic = ProcessorStateManager.StoreChangelogTopic(context.ApplicationId, Name);
    //        changeLogger = new StoreChangeLogger<>(
    //            name,
    //            context,
    //            new StateSerdes<>(topic, Serdes.ByteArray(), Serdes.ByteArray()));

    //        // if the inner store is an LRU cache,.Add the eviction listener to log removed record
    //        if (wrapped is MemoryLRUCache)
    //        {
    //            ((MemoryLRUCache)wrapped).setWhenEldestRemoved((key, value) =>
    //{
    //    // pass null to indicate removal
    //    log(key, null);
    //});
    //        }
        }

        public long approximateNumEntries
            => Wrapped.approximateNumEntries;

        public void Add(
            Bytes key,
            byte[] value)
        {
            Wrapped.Add(key, value);
            Log(key, value);
        }

        public byte[] PutIfAbsent(
            Bytes key,
            byte[] value)
        {
            var previous = Wrapped.PutIfAbsent(key, value);
            if (previous == null)
            {
                // then it was absent
                Log(key, value);
            }

            return previous;
        }

        public void PutAll(List<KeyValuePair<Bytes, byte[]>> entries)
        {
            Wrapped.PutAll(entries);
            foreach (KeyValuePair<Bytes, byte[]> entry in entries)
            {
                Log(entry.Key, entry.Value);
            }
        }

        public byte[] Delete(Bytes key)
        {
            var oldValue = Wrapped.Delete(key);
            Log(key, null);
            return oldValue;
        }

        public byte[] Get(Bytes key)
        {
            return Wrapped.Get(key);
        }

        public IKeyValueIterator<Bytes, byte[]> Range(
            Bytes from,
            Bytes to)
        {
            return Wrapped.Range(from, to);
        }

        public IKeyValueIterator<Bytes, byte[]> All()
        {
            return Wrapped.All();
        }

        void Log(Bytes key, byte[] value)
        {
            changeLogger.LogChange(key, value);
        }
    }
}
