using Kafka.Streams.State.Interfaces;
using Kafka.Streams.State.Internals;
using System.Collections.Generic;

namespace Kafka.Streams.Internals
{
    public interface IKeySchema
    {
        /**
         * Given a range of record keys and a time, construct a Segmented key that represents
         * the upper range of keys to search when performing range queries.
         * @see SessionKeySchema#upperRange
         * @see WindowKeySchema#upperRange
         * @param key
         * @param to
         * @return      The key that represents the upper range to search for in the store
         */
        Bytes UpperRange(Bytes key, long to);

        /**
         * Given a range of record keys and a time, construct a Segmented key that represents
         * the lower range of keys to search when performing range queries.
         * @see SessionKeySchema#lowerRange
         * @see WindowKeySchema#lowerRange
         * @param key
         * @param from
         * @return      The key that represents the lower range to search for in the store
         */
        Bytes LowerRange(Bytes key, long from);

        /**
         * Given a range of fixed size record keys and a time, construct a Segmented key that represents
         * the upper range of keys to search when performing range queries.
         * @see SessionKeySchema#upperRange
         * @see WindowKeySchema#upperRange
         * @param key the last key in the range
         * @param to the last timestamp in the range
         * @return The key that represents the upper range to search for in the store
         */
        Bytes UpperRangeFixedSize(Bytes key, long to);

        /**
         * Given a range of fixed size record keys and a time, construct a Segmented key that represents
         * the lower range of keys to search when performing range queries.
         * @see SessionKeySchema#lowerRange
         * @see WindowKeySchema#lowerRange
         * @param key the first key in the range
         * @param from the first timestamp in the range
         * @return      The key that represents the lower range to search for in the store
         */
        Bytes LowerRangeFixedSize(Bytes key, long from);

        /**
         * Extract the timestamp of the segment from the key. The key is a composite of
         * the record-key, any timestamps, plus any additional information.
         * @see SessionKeySchema#lowerRange
         * @see WindowKeySchema#lowerRange
         * @param key
         * @return
         */
        long SegmentTimestamp(Bytes key);

        /**
         * Create an implementation of {@link HasNextCondition} that knows when
         * to stop iterating over the KeyValueSegments. Used during {@link SegmentedBytesStore#Fetch(Bytes, Bytes, long, long)} operations
         * @param binaryKeyFrom the first key in the range
         * @param binaryKeyTo   the last key in the range
         * @param from          starting time range
         * @param to            ending time range
         * @return
         */
        bool HasNextCondition(Bytes binaryKeyFrom, Bytes binaryKeyTo, long from, long to);

        /**
         * Used during {@link SegmentedBytesStore#Fetch(Bytes, long, long)} operations to determine
         * which segments should be scanned.
         * @param segments
         * @param from
         * @param to
         * @return  List of segments to search
         */
        List<S> SegmentsToSearch<S>(ISegments<S> segments, long from, long to)
            where S : ISegment;
    }
}
