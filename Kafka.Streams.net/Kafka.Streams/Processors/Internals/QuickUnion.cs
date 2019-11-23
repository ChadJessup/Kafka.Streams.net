/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for.Additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class QuickUnion<T>
    {
        private readonly Dictionary<T, T> ids = new Dictionary<T, T>();

        public void Add(T id)
        {
            ids.Add(id, id);
        }

        public bool Exists(T id)
        {
            return ids.ContainsKey(id);
        }

        /**
         * @throws NoSuchElementException if the parent of this node is null
         */
        public T Root(T id)
        {
            T current = id;
            T parent = ids[current];

            if (parent == null)
            {
                throw new KeyNotFoundException("id: " + id.ToString());
            }

            while (!parent.Equals(current))
            {
                // do the path splitting
                T grandparent = ids[parent];
                if (ids.ContainsKey(current))
                {
                    ids[current] = grandparent;
                }
                else
                {
                    ids.Add(current, grandparent);
                }

                current = parent;
                parent = grandparent;
            }
            return current;
        }

        public void Unite(T id1, T[] idList)
        {
            foreach (T id2 in idList)
            {
                UnitePair(id1, id2);
            }
        }

        private void UnitePair(T id1, T id2)
        {
            T root1 = Root(id1);
            T root2 = Root(id2);

            if (!root1.Equals(root2))
            {
                if (ids.ContainsKey(root1))
                {
                    ids[root1] = root2;
                }
                else
                {
                    ids.Add(root1, root2);
                }
            }
        }
    }
}