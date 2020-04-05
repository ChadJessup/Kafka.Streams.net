
//using Kafka.Streams.Errors;
//using Kafka.Streams.Processors.Interfaces;
//using Kafka.Streams.State.Interfaces;
//using Microsoft.Extensions.Logging;
//using System.Collections.Generic;
//using System.IO;

//namespace Kafka.Streams.State.Internals
//{

//    public abstract class AbstractSegments<S> : Segments<S>
//        where S : ISegment
//    {
//        private static ILogger log = new LoggerFactory().CreateLogger<AbstractSegments<S>>();

//        Dictionary<long, S> segments = new Dictionary<long, S>();
//        string name;
//        private long retentionPeriod;
//        private long segmentInterval;
//        private SimpleDateFormat formatter;

//        public AbstractSegments(
//            string name,
//            long retentionPeriod,
//            long segmentInterval)
//        {
//            this.name = name;
//            this.segmentInterval = segmentInterval;
//            this.retentionPeriod = retentionPeriod;
//            // Create a date formatter. Formatted timestamps are used as segment name suffixes
//            this.formatter = new SimpleDateFormat("yyyyMMddHHmm");
//            this.formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
//        }

//        public long segmentId(long timestamp)
//        {
//            return timestamp / segmentInterval;
//        }

//        public string segmentName(long segmentId)
//        {
//            // (1) previous string.Format used - as a separator so if this changes in the future
//            // then we should use something different.
//            // (2) previous string.Format used : as a separator (which did break KafkaStreams on Windows OS)
//            // so if this changes in the future then we should use something different.
//            return name + "." + segmentId * segmentInterval;
//        }

//        public S getSegmentForTimestamp(long timestamp)
//        {
//            return segments[segmentId(timestamp)];
//        }

//        public override S getOrCreateSegmentIfLive(
//            long segmentId,
//            IInternalProcessorContext<K, V> context,
//            long streamTime)
//        {
//            long minLiveTimestamp = streamTime - retentionPeriod;
//            long minLiveSegment = segmentId(minLiveTimestamp);

//            S toReturn;
//            if (segmentId >= minLiveSegment)
//            {
//                // The segment is live. get it, ensure it's open, and return it.
//                toReturn = getOrCreateSegment(segmentId, context);
//            }
//            else
//            {
//                toReturn = default;
//            }

//            cleanupEarlierThan(minLiveSegment);
//            return toReturn;
//        }

//        public override void openExisting(IInternalProcessorContext<K, V> context, long streamTime)
//        {
//            try
//            {
//                FileInfo dir = new FileInfo(context.stateDir(), name);
//                if (dir.Exists)
//                {
//                    string[] list = dir.list();
//                    if (list != null)
//                    {
//                        long[] segmentIds = new long[list.Length];
//                        for (int i = 0; i < list.Length; i++)
//                        {
//                            segmentIds[i] = segmentIdFromSegmentName(list[i], dir);
//                        }

//                        // open segments in the id order
//                        Arrays.sort(segmentIds);
//                        foreach (long segmentId in segmentIds)
//                        {
//                            if (segmentId >= 0)
//                            {
//                                getOrCreateSegment(segmentId, context);
//                            }
//                        }
//                    }
//                }
//                else
//                {
//                    if (!dir.mkdir())
//                    {
//                        throw new ProcessorStateException(string.Format("dir %s doesn't exist and cannot be created for segments %s", dir, name));
//                    }
//                }
//            }
//            catch (Exception ex)
//            {
//                // ignore
//            }

//            long minLiveSegment = segmentId(streamTime - retentionPeriod);
//            cleanupEarlierThan(minLiveSegment);
//        }

//        public override List<S> segments(long timeFrom, long timeTo)
//        {
//            List<S> result = new List<S>();
//            NavigableMap<long, S> segmentsInRange = segments.subMap(
//                segmentId(timeFrom), true,
//                segmentId(timeTo), true
//            );
//            foreach (S segment in segmentsInRange.Values)
//            {
//                if (segment.isOpen())
//                {
//                    result.Add(segment);
//                }
//            }
//            return result;
//        }

//        public override List<S> allSegments()
//        {
//            List<S> result = new List<S>();

//            foreach (S segment in segments.Values)
//            {
//                if (segment.isOpen())
//                {
//                    result.Add(segment);
//                }
//            }
//            return result;
//        }

//        public void flush()
//        {
//            foreach (S segment in segments.Values)
//            {
//                segment.flush();
//            }
//        }

//        public void close()
//        {
//            foreach (S segment in segments.Values)
//            {
//                segment.close();
//            }
//            segments.clear();
//        }

//        private void cleanupEarlierThan(long minLiveSegment)
//        {
//            IEnumerator<KeyValuePair<long, S>> toRemove =
//                segments.headMap(minLiveSegment, false).iterator();

//            while (toRemove.hasNext())
//            {
//                var next = toRemove.MoveNext();
//                toRemove.Remove();
//                S segment = next.Value;
//                segment.close();
//                try
//                {
//                    segment.Destroy();
//                }
//                catch (IOException e)
//                {
//                    log.LogError("Error destroying {}", segment, e);
//                }
//            }
//        }

//        private long segmentIdFromSegmentName(
//            string segmentName,
//            FileInfo parent)
//        {
//            int segmentSeparatorIndex = name.Length;
//            char segmentSeparator = segmentName.charAt(segmentSeparatorIndex);
//            string segmentIdString = segmentName.Substring(segmentSeparatorIndex + 1);
//            long segmentId;

//            // old style segment name with date
//            if (segmentSeparator == '-')
//            {
//                try
//                {
//                    segmentId = formatter.parse(segmentIdString).getTime() / segmentInterval;
//                }
//                catch (ParseException e)
//                {
//                    log.LogWarning("Unable to parse segmentName {} to a date. This segment will be skipped", segmentName);
//                    return -1L;
//                }
//                renameSegmentFile(parent, segmentName, segmentId);
//            }
//            else
//            {
//                // for both new formats (with : or .) parse segment ID identically
//                try
//                {
//                    segmentId = long.Parse(segmentIdString) / segmentInterval;
//                }
//                catch (NumberFormatException e)
//                {
//                    throw new ProcessorStateException("Unable to parse segment id as long from segmentName: " + segmentName);
//                }

//                // intermediate segment name with : breaks KafkaStreams on Windows OS => rename segment file to new name with .
//                if (segmentSeparator == ':')
//                {
//                    renameSegmentFile(parent, segmentName, segmentId);
//                }
//            }

//            return segmentId;

//        }

//        private void renameSegmentFile(
//            FileInfo parent,
//            string segmentName,
//            long segmentId)
//        {
//            FileInfo newName = new FileInfo(parent, segmentName(segmentId));
//            FileInfo oldName = new FileInfo(parent, segmentName);
//            if (!oldName.renameTo(newName))
//            {
//                throw new ProcessorStateException("Unable to rename old style segment from: "
//                    + oldName
//                    + " to new name: "
//                    + newName);
//            }
//        }

//    }
//}