
//namespace Kafka.Streams.State.Internals
//{
//    public class RocksDbTimestampedSegmentedBytesStore : AbstractRocksDbSegmentedBytesStore<TimestampedSegment>
//    {

//        public RocksDbTimestampedSegmentedBytesStore(string name,
//                                              string metricScope,
//                                              long retention,
//                                              long segmentInterval,
//                                              KeySchema keySchema)
//            : base(name, metricScope, keySchema, new TimestampedSegments(name, retention, segmentInterval))
//        {
//        }
//    }
//}