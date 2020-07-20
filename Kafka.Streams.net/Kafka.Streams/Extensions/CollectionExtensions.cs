using System;
using System.Collections.Generic;
using System.Text;

namespace System.Collections.Generic
{
    public static class CollectionExtensions
    {
        public static void AddRange<T>(this ICollection<T> collection, IEnumerable<T> items)
        {
            if (collection is null)
            {
                throw new ArgumentNullException(nameof(collection));
            }

            if (items is null)
            {
                throw new ArgumentNullException(nameof(items));
            }

            foreach (var item in items)
            {
                collection.Add(item);
            }
        }

        public static void ForEach<T>(this ICollection<T> collection, Action<T> action)
        {
            if (collection is null)
            {
                throw new ArgumentNullException(nameof(collection));
            }

            if (action is null)
            {
                throw new ArgumentNullException(nameof(action));
            }

            foreach (var element in collection)
            {
                action(element);
            }
        }
    }
}
