using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Temporary
{
    public static class Arrays
    {
        public static List<T> asList<T>(params T[] items)
            => items.ToList();
    }
}
