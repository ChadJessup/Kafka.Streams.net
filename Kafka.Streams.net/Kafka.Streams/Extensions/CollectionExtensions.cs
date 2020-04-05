using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Extensions
{
    public static class CollectionExtensions
    {
        public static string ToJoinedString<T>(this IEnumerable<T> collection, char separator = ',')
           => string.Join(separator, collection ?? Enumerable.Empty<T>());
    }
}

namespace System
{
    public static class ArrayExtensions
    {
        public static List<T> AsList<T>(this Array _, params T[] items)
        {
            var list = new List<T>();

            list.AddRange(items);

            return list;
        }
    }
}
