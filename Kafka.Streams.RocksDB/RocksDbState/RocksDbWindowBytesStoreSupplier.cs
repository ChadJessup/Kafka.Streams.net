using Kafka.Streams.State.Windowed;
using System;

namespace Kafka.Streams.State.Internals
{
    public class RocksDbWindowBytesStoreSupplier : IWindowBytesStoreSupplier
    {
        public string Name { get; }
        public TimeSpan RetentionPeriod { get; }
        public TimeSpan SegmentInterval { get; }
        public TimeSpan WindowSize { get; }
        public bool RetainDuplicates { get; }
        public bool ReturnTimestampedStore { get; }

        public RocksDbWindowBytesStoreSupplier(
            string Name,
            TimeSpan retentionPeriod,
            TimeSpan segmentInterval,
            TimeSpan windowSize,
            bool retainDuplicates,
            bool returnTimestampedStore)
        {
            this.Name = Name;
            this.RetentionPeriod = retentionPeriod;
            this.SegmentInterval = segmentInterval;
            this.WindowSize = windowSize;
            this.RetainDuplicates = retainDuplicates;
            this.ReturnTimestampedStore = returnTimestampedStore;
        }

        public IWindowStore<Bytes, byte[]> Get()
        {
            if (!this.ReturnTimestampedStore)
            {
                return null;
                // new RocksDbWindowStore(
                // new RocksDbSegmentedBytesStore(
                //     Name,
                //     //metricsScope(),
                //     RetentionPeriod,
                //     SegmentInterval,
                //     new WindowKeySchema()),
                // RetainDuplicates,
                // WindowSize);
            }
            else
            {
                return null;
                //new RocksDbTimestampedWindowStore(
                //new RocksDbTimestampedSegmentedBytesStore(
                //    Name,
                //    //metricsScope(),
                //    RetentionPeriod,
                //    SegmentInterval,
                //    new WindowKeySchema()),
                //RetainDuplicates,
                //WindowSize);
            }
        }

        // public string MetricsScope()
        // {
        //     return "rocksdb-window-state";
        // }

        public int Segments()
        {
            throw new System.NotImplementedException();
        }

        public void SetName(string Name)
        {
            throw new NotImplementedException();
        }
    }
}
