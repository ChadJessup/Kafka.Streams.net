namespace Kafka.Streams.Tests.State.Internals
{
    /*






    *

    *





    */




    public class RocksDBTimestampedSegmentedBytesStoreTest
        : AbstractRocksDBSegmentedBytesStoreTest<TimestampedSegment>
    {

        RocksDBTimestampedSegmentedBytesStore GetBytesStore()
        {
            return new RocksDBTimestampedSegmentedBytesStore(
                storeName,
                "metrics-scope",
                retention,
                segmentInterval,
                schema
            );
        }


        TimestampedSegments NewSegments()
        {
            return new TimestampedSegments(storeName, retention, segmentInterval);
        }


        Options GetOptions(TimestampedSegment segment)
        {
            return segment.getOptions();
        }
    }
}
/*






*

*





*/




