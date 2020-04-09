using System;
using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.KeyValues;
using Kafka.Streams.State.Sessions;
using Kafka.Streams.State.Windowed;

namespace Kafka.Streams.NullModels
{
    public class NullStoreSupplier : 
        ITimestampedKeyValueBytesStoreSupplier,
        IWindowBytesStoreSupplier,
        ISessionBytesStoreSupplier
    {
        public string Name { get; private set; } = nameof(NullStoreSupplier);
        public TimeSpan SegmentInterval { get; }
        public TimeSpan WindowSize { get; }
        public bool RetainDuplicates { get; }
        public TimeSpan RetentionPeriod { get; }

        public IKeyValueStore<Bytes, byte[]> Get()
        {
            return new NullKeyValueStore();
        }

        public long SegmentIntervalMs() => 0;
        public int Segments() => 0;
        public void SetName(string name) => this.Name = name;

        IWindowStore<Bytes, byte[]> IStoreSupplier<IWindowStore<Bytes, byte[]>>.Get()
            => new NullWindowStore();

        ISessionStore<Bytes, byte[]> IStoreSupplier<ISessionStore<Bytes, byte[]>>.Get()
            => new NullSessionStore();
    }
}
