/*






 *

 *





 */




public class RocksDBTimestampedSegmentedBytesStoreTest
    : AbstractRocksDBSegmentedBytesStoreTest<TimestampedSegment> {

    RocksDBTimestampedSegmentedBytesStore getBytesStore() {
        return new RocksDBTimestampedSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );
    }

    
    TimestampedSegments newSegments() {
        return new TimestampedSegments(storeName, retention, segmentInterval);
    }

    
    Options getOptions(TimestampedSegment segment) {
        return segment.getOptions();
    }
}
