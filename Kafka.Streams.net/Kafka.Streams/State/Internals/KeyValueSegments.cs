
//    public class KeyValueSegments : AbstractSegments<KeyValueSegment>
//    {

//        KeyValueSegments(string name,
//                         long retentionPeriod,
//                         long segmentInterval)
//        {
//            base(name, retentionPeriod, segmentInterval);
//        }

//        public override KeyValueSegment getOrCreateSegment(long segmentId,
//                                                  IInternalProcessorContext<K, V> context)
//        {
//            if (segments.ContainsKey(segmentId))
//            {
//                return segments[segmentId];
//            }
//            else
//            {
//                KeyValueSegment newSegment = new KeyValueSegment(segmentName(segmentId), name, segmentId);

//                if (segments.Add(segmentId, newSegment) != null)
//                {
//                    throw new InvalidOperationException("KeyValueSegment already exists. Possible concurrent access.");
//                }

//                newSegment.openDB(context);
//                return newSegment;
//            }
//        }
//    }
//}