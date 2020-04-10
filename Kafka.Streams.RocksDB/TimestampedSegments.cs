
//    public class TimestampedSegments : AbstractSegments<TimestampedSegment>
//    {

//        TimestampedSegments(string Name,
//                            long retentionPeriod,
//                            long segmentInterval)
//            : base(Name, retentionPeriod, segmentInterval)
//        {
//        }

//        public override TimestampedSegment getOrCreateSegment(long segmentId,
//                                                     IInternalProcessorContext<K, V> context)
//        {
//            if (segments.ContainsKey(segmentId))
//            {
//                return segments[segmentId];
//            }
//            else
//            {
//                TimestampedSegment newSegment = new TimestampedSegment(segmentName(segmentId), Name, segmentId);

//                if (segments.Add(segmentId, newSegment) != null)
//                {
//                    throw new InvalidOperationException("TimestampedSegment already exists. Possible concurrent access.");
//                }

//                newSegment.openDB(context);
//                return newSegment;
//            }
//        }
//    }
//}