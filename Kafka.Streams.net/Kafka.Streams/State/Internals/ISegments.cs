using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.State.Internals
{
    public interface ISegments<S>
        where S : ISegment
    {
        long SegmentId(long timestamp);
        string SegmentName(long segmentId);
        S GetSegmentForTimestamp(long timestamp);
        S GetOrCreateSegmentIfLive(long segmentId, IInternalProcessorContext context, long streamTime);
        S GetOrCreateSegment(long segmentId, IInternalProcessorContext context);
        void OpenExisting(IInternalProcessorContext context, long streamTime);
        List<S> GetSegments(long timeFrom, long timeTo);
        List<S> AllSegments();
        void Flush();
        void Close();
    }
}
