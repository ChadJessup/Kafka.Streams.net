using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class StreamsGraphNodeComparer : IComparer<float>
    {
        public int Compare(float x, float y)
            => x.CompareTo(y);
    }
}
