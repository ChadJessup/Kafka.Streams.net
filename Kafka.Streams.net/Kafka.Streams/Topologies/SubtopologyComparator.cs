using System;
using System.Collections.Generic;

namespace Kafka.Streams.Topologies
{
    [Serializable]
    public class SubtopologyComparator : IComparer<ISubtopology>
    {
        public int Compare(ISubtopology subtopology1, ISubtopology subtopology2)
        {
            if (subtopology1.Equals(subtopology2))
            {
                return 0;
            }

            return subtopology1.id - subtopology2.id;
        }
    }
}