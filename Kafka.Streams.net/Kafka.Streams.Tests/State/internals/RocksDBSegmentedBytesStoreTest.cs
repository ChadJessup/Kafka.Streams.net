namespace Kafka.Streams.Tests.State.Internals
{
    /*






    *

    *





    */




    public class RocksDBSegmentedBytesStoreTest : AbstractRocksDBSegmentedBytesStoreTest<KeyValueSegment>
    {


        RocksDBSegmentedBytesStore GetBytesStore()
        {
            return new RocksDBSegmentedBytesStore(
                storeName,
                "metrics-scope",
                retention,
                segmentInterval,
                schema
            );
        }


        KeyValueSegments NewSegments()
        {
            return new KeyValueSegments(storeName, retention, segmentInterval);
        }


        Options GetOptions(KeyValueSegment segment)
        {
            return segment.getOptions();
        }
    }
}
/*






*

*





*/




