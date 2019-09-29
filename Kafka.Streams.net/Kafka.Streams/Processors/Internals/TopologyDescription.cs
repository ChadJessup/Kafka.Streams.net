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
using Kafka.Common;
using Kafka.Streams.Interfaces;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Kafka.Streams.Topologies;
using Kafka.Streams.State;

namespace Kafka.Streams.Topologies
{
    public class TopologyDescription
    {
        private readonly SortedSet<ISubtopology> subtopologies = new SortedSet<ISubtopology>(/*SUBTOPOLOGY_COMPARATOR*/);
        private readonly SortedSet<IGlobalStore> globalStores = new SortedSet<IGlobalStore>(/*GLOBALSTORE_COMPARATOR*/);

        public void addSubtopology(ISubtopology subtopology)
        {
            subtopologies.Add(subtopology);
        }

        public void addGlobalStore(IGlobalStore globalStore)
        {
            globalStores.Add(globalStore);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("Topologies:\n ");
            ISubtopology[] sortedSubtopologies =
                subtopologies.OrderByDescending(t => t.id).ToArray();
            IGlobalStore[] sortedGlobalStores =
                globalStores.OrderByDescending(s => s.id).ToArray();
            int expectedId = 0;
            int subtopologiesIndex = sortedSubtopologies.Length - 1;
            int globalStoresIndex = sortedGlobalStores.Length - 1;
            while (subtopologiesIndex != -1 && globalStoresIndex != -1)
            {
                sb.Append("  ");
                ISubtopology subtopology = sortedSubtopologies[subtopologiesIndex];
                IGlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
                if (subtopology.id == expectedId)
                {
                    sb.Append(subtopology);
                    subtopologiesIndex--;
                }
                else
                {
                    sb.Append(globalStore);
                    globalStoresIndex--;
                }
                expectedId++;
            }

            while (subtopologiesIndex != -1)
            {
                ISubtopology subtopology = sortedSubtopologies[subtopologiesIndex];
                sb.Append("  ");
                sb.Append(subtopology);
                subtopologiesIndex--;
            }
            while (globalStoresIndex != -1)
            {
                IGlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
                sb.Append("  ");
                sb.Append(globalStore);
                globalStoresIndex--;
            }
            return sb.ToString();
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            TopologyDescription that = (TopologyDescription)o;
            return subtopologies.Equals(that.subtopologies)
                && globalStores.Equals(that.globalStores);
        }

        public override int GetHashCode()
        {
            return (subtopologies, globalStores).GetHashCode();
        }

    }
}