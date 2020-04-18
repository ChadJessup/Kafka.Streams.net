using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Temporary
{
    public static class Arrays
    {
        public static List<T> asList<T>(params T[] items)
            => items.ToList();

        public static IEnumerable<T> asItems<T>(params T[] items)
            => Arrays.asList<T>(items);
    }
}
