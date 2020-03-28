using Microsoft.Extensions.DependencyInjection;
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
