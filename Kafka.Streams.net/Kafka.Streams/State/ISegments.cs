using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public interface ISegments<S>
        where S : ISegment
    {
        long SegmentId(DateTime timestamp);
        string SegmentName(long segmentId);
        S GetSegmentForTimestamp(DateTime timestamp);
        S GetOrCreateSegmentIfLive(long segmentId, IInternalProcessorContext context, DateTime streamTime);
        S GetOrCreateSegment(long segmentId, IInternalProcessorContext context);
        void OpenExisting(IInternalProcessorContext context, DateTime streamTime);
        List<S> GetSegments(DateTime timeFrom, DateTime timeTo);
        List<S> AllSegments();
        void Flush();
        void Close();
    }
}
