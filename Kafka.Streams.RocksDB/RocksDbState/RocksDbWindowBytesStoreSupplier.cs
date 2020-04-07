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
            string name,
            TimeSpan retentionPeriod,
            TimeSpan segmentInterval,
            TimeSpan windowSize,
            bool retainDuplicates,
            bool returnTimestampedStore)
        {
            this.Name = name;
            this.RetentionPeriod = retentionPeriod;
            this.SegmentInterval = segmentInterval;
            this.WindowSize = windowSize;
            this.RetainDuplicates = retainDuplicates;
            this.ReturnTimestampedStore = returnTimestampedStore;
        }

        public IWindowStore<Bytes, byte[]> Get()
        {
            if (!ReturnTimestampedStore)
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

        public void SetName(string name)
        {
            throw new NotImplementedException();
        }
    }
}
