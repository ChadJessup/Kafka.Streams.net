using System;
using System.Collections.Generic;

namespace Kafka.Streams.Topologies
{
    [Serializable]
    public class GlobalStoreComparator : IComparer<IGlobalStore>
    {
        public int Compare(IGlobalStore globalStore1, IGlobalStore globalStore2)
        {
            if (globalStore1.Equals(globalStore2))
            {
                return 0;
            }

            return globalStore1.id - globalStore2.id;
        }
    }
}