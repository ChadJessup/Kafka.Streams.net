using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class QuickUnion<T>
    {
        private readonly Dictionary<T, T> ids = new Dictionary<T, T>();

        public void Add(T id)
        {
            this.ids.Add(id, id);
        }

        public bool Exists(T id)
        {
            return this.ids.ContainsKey(id);
        }

        public T Root(T id)
        {
            id = id ?? throw new ArgumentNullException(nameof(id));

            T current = id;

            if (!this.ids.TryGetValue(current, out var parent))
            {
                throw new KeyNotFoundException("id: " + id.ToString());
            }

            while (!parent?.Equals(current) ?? false)
            {
                // do the path splitting
                T grandparent = this.ids[parent];
                if (this.ids.ContainsKey(current))
                {
                    this.ids[current] = grandparent;
                }
                else
                {
                    this.ids.Add(current, grandparent);
                }

                current = parent;
                parent = grandparent;
            }

            return current;
        }

        public void Unite(T id, T singleId)
            => this.Unite(id, new[] { singleId });

        public void Unite(T id1, params T[] idList)
        {
            foreach (T id2 in idList ?? Array.Empty<T>())
            {
                this.UnitePair(id1, id2);
            }
        }

        private void UnitePair(T id1, T id2)
        {
            T root1 = this.Root(id1);
            T root2 = this.Root(id2);

            if (!root1?.Equals(root2) ?? false)
            {
                if (this.ids.ContainsKey(root1))
                {
                    this.ids[root1] = root2;
                }
                else
                {
                    this.ids.Add(root1, root2);
                }
            }
        }
    }
}