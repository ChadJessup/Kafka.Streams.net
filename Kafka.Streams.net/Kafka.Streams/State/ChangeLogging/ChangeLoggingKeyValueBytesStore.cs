using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State.KeyValues;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public class ChangeLoggingKeyValueBytesStore
        : WrappedStateStore<IKeyValueStore<Bytes, byte[]>>, IKeyValueStore<Bytes, byte[]>
    {

        public StoreChangeLogger<Bytes, byte[]> changeLogger { get; }

        public ChangeLoggingKeyValueBytesStore(IKeyValueStore<Bytes, byte[]> inner)
            : base(inner)
        {
        }

        public override void init(
            IProcessorContext context,
            IStateStore root)
        {
            base.init(context, root);
            var topic = ProcessorStateManager.storeChangelogTopic(context.applicationId, name);
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
            => wrapped.approximateNumEntries;

        public void Add(
            Bytes key,
            byte[] value)
        {
            wrapped.Add(key, value);
            log(key, value);
        }

        public byte[] putIfAbsent(
            Bytes key,
            byte[] value)
        {
            var previous = wrapped.putIfAbsent(key, value);
            if (previous == null)
            {
                // then it was absent
                log(key, value);
            }

            return previous;
        }

        public void putAll(List<KeyValue<Bytes, byte[]>> entries)
        {
            wrapped.putAll(entries);
            foreach (KeyValue<Bytes, byte[]> entry in entries)
            {
                log(entry.Key, entry.Value);
            }
        }

        public byte[] delete(Bytes key)
        {
            var oldValue = wrapped.delete(key);
            log(key, null);
            return oldValue;
        }

        public byte[] get(Bytes key)
        {
            return wrapped.get(key);
        }

        public IKeyValueIterator<Bytes, byte[]> range(
            Bytes from,
            Bytes to)
        {
            return wrapped.range(from, to);
        }

        public IKeyValueIterator<Bytes, byte[]> all()
        {
            return wrapped.all();
        }

        void log(Bytes key, byte[] value)
        {
            changeLogger.logChange(key, value);
        }
    }
}