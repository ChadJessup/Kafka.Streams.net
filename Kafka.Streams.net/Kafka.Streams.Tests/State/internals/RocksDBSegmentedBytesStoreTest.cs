/*






 *

 *





 */




public class RocksDBSegmentedBytesStoreTest : AbstractRocksDBSegmentedBytesStoreTest<KeyValueSegment> {

    
    RocksDBSegmentedBytesStore getBytesStore() {
        return new RocksDBSegmentedBytesStore(
            storeName,
            "metrics-scope",
            retention,
            segmentInterval,
            schema
        );
    }

    
    KeyValueSegments newSegments() {
        return new KeyValueSegments(storeName, retention, segmentInterval);
    }

    
    Options getOptions(KeyValueSegment segment) {
        return segment.getOptions();
    }
}
