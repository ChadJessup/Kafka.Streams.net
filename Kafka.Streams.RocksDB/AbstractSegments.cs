using Kafka.Streams.Errors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Interfaces;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Kafka.Streams.State.Internals
{
    public abstract class AbstractSegments<S> : ISegments<S>
        where S : ISegment
    {
        private readonly ILogger<AbstractSegments<S>> logger;
        public Dictionary<long, S> Segments { get; } = new Dictionary<long, S>();
        private readonly SimpleDateFormat formatter;
        private readonly long retentionPeriod;
        private readonly long segmentInterval;
        private readonly string name;

        public AbstractSegments(
            ILogger<AbstractSegments<S>> logger,
            string name,
            long retentionPeriod,
            long segmentInterval)
        {
            this.logger = logger;
            this.name = name;
            this.segmentInterval = segmentInterval;
            this.retentionPeriod = retentionPeriod;
            // Create a date formatter. Formatted timestamps are used as segment name suffixes
            this.formatter = new SimpleDateFormat("yyyyMMddHHmm");
            this.formatter.SetTimeZone(new SimpleTimeZone(0, "UTC"));
        }

        public long SegmentId(long timestamp)
        {
            return timestamp / segmentInterval;
        }

        public string SegmentName(long segmentId)
        {
            // (1) previous string.Format used - as a separator so if this changes in the future
            // then we should use something different.
            // (2) previous string.Format used : as a separator (which did break KafkaStreams on Windows OS)
            // so if this changes in the future then we should use something different.
            return name + "." + segmentId * segmentInterval;
        }

        public S GetSegmentForTimestamp(long timestamp)
        {
            return this.Segments[SegmentId(timestamp)];
        }

        public S GetOrCreateSegmentIfLive(
            long segmentId,
            IInternalProcessorContext context,
            long streamTime)
        {
            long minLiveTimestamp = streamTime - retentionPeriod;
            long minLiveSegment = SegmentId(minLiveTimestamp);

            S toReturn;
            if (segmentId >= minLiveSegment)
            {
                // The segment is live. get it, ensure it's open, and return it.
                toReturn = GetOrCreateSegment(segmentId, context);
            }
            else
            {
                toReturn = default;
            }

            CleanupEarlierThan(minLiveSegment);

            return toReturn;
        }

        public void OpenExisting(IInternalProcessorContext context, long streamTime)
        {
            try
            {
                var dir = new DirectoryInfo(Path.Combine(context.StateDir.FullName, name));
                if (dir.Exists)
                {
                    var list = dir.GetFiles();
                    if (list != null)
                    {
                        long[] segmentIds = new long[list.Length];
                        for (int i = 0; i < list.Length; i++)
                        {
                            segmentIds[i] = SegmentIdFromSegmentName(list[i].FullName, dir);
                        }

                        // open segments in the id order
                        Array.Sort(segmentIds);
                        foreach (long segmentId in segmentIds)
                        {
                            if (segmentId >= 0)
                            {
                                GetOrCreateSegment(segmentId, context);
                            }
                        }
                    }
                }
                else
                {
                    dir.Create();
                }
            }
            catch (Exception)
            {
                // ignore
            }

            long minLiveSegment = SegmentId(streamTime - retentionPeriod);
            CleanupEarlierThan(minLiveSegment);
        }

        public List<S> GetSegments(long timeFrom, long timeTo)
        {
            List<S> result = new List<S>();

            foreach (var segment in this.Segments.Where(s => s.Key >= SegmentId(timeFrom) && s.Key <= SegmentId(timeTo)))
            {
                if (segment.Value.IsOpen())
                {
                    result.Add(segment.Value);
                }
            }
            return result;
        }

        public List<S> AllSegments()
        {
            List<S> result = new List<S>();

            foreach (S segment in this.Segments.Values)
            {
                if (segment.IsOpen())
                {
                    result.Add(segment);
                }
            }
            return result;
        }

        public void Flush()
        {
            foreach (S segment in this.Segments.Values)
            {
                segment.Flush();
            }
        }

        public void Close()
        {
            foreach (S segment in this.Segments.Values)
            {
                segment.Close();
            }

            this.Segments.Clear();
        }

        private void CleanupEarlierThan(long minLiveSegment)
        {
            IEnumerator<KeyValuePair<long, S>> toRemove =
                this.Segments.Where(s => s.Key <= minLiveSegment).GetEnumerator();

            while (toRemove.MoveNext())
            {
                var next = toRemove.Current;
                // toRemove.Current.Remove();
                S segment = next.Value;
                segment.Close();
                try
                {
                    segment.Destroy();
                }
                catch (IOException e)
                {
                    this.logger.LogError("Error destroying {}", segment, e);
                }
            }
        }

        private long SegmentIdFromSegmentName(
            string segmentName,
            DirectoryInfo parent)
        {
            int segmentSeparatorIndex = name.Length;
            char segmentSeparator = segmentName[segmentSeparatorIndex];
            string segmentIdString = segmentName.Substring(segmentSeparatorIndex + 1);
            long segmentId;

            // old style segment name with date
            if (segmentSeparator == '-')
            {
                try
                {
                    // segmentId = formatter.Parse(segmentIdString).getTime() / segmentInterval;
                    segmentId = 0;// segmentIdString / segmentInterval;
                }
                catch (Exception e)
                {
                    this.logger.LogWarning($"Unable to parse segmentName {segmentName} to a date. This segment will be skipped");
                    return -1L;
                }

                RenameSegmentFile(parent, segmentName, segmentId);
            }
            else
            {
                // for both new formats (with : or .) parse segment ID identically
                try
                {
                    segmentId = long.Parse(segmentIdString) / segmentInterval;
                }
                catch (Exception e)
                {
                    throw new ProcessorStateException("Unable to parse segment id as long from segmentName: " + segmentName);
                }

                // intermediate segment name with : breaks KafkaStreams on Windows OS => rename segment file to new name with .
                if (segmentSeparator == ':')
                {
                    RenameSegmentFile(parent, segmentName, segmentId);
                }
            }

            return segmentId;
        }

        private void RenameSegmentFile(
            DirectoryInfo parent,
            string segmentName,
            long segmentId)
        {
            FileInfo newName = new FileInfo(Path.Combine(parent.FullName, SegmentName(segmentId)));
            FileInfo oldName = new FileInfo(Path.Combine(parent.FullName, segmentName));

            try
            {
                oldName.MoveTo(newName.FullName);
            }
            catch
            {
                throw new ProcessorStateException("Unable to rename old style segment from: "
                    + oldName
                    + " to new name: "
                    + newName);
            }
        }

        public S GetOrCreateSegment(long segmentId, IInternalProcessorContext context)
        {
            throw new NotImplementedException();
        }
    }
}
