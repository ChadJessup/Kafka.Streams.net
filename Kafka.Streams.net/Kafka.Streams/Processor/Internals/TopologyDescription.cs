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
using System.Collections.Generic;

public static class TopologyDescription : TopologyDescription
{

    private TreeSet<TopologyDescription.Subtopology> subtopologies = new TreeSet<>(SUBTOPOLOGY_COMPARATOR);
    private TreeSet<TopologyDescription.GlobalStore> globalStores = new TreeSet<>(GLOBALSTORE_COMPARATOR);

    public void addSubtopology(TopologyDescription.Subtopology subtopology)
    {
        subtopologies.Add(subtopology);
    }

    public void addGlobalStore(TopologyDescription.GlobalStore globalStore)
    {
        globalStores.Add(globalStore);
    }


    public HashSet<TopologyDescription.Subtopology> subtopologies()
    {
        return Collections.unmodifiableSet(subtopologies);
    }


    public HashSet<TopologyDescription.GlobalStore> globalStores()
    {
        return Collections.unmodifiableSet(globalStores);
    }


    public string ToString()
    {
        StringBuilder sb = new StringBuilder();
        sb.Append("Topologies:\n ");
        TopologyDescription.Subtopology[] sortedSubtopologies =
            subtopologies.descendingSet().toArray(new Subtopology[0]);
        TopologyDescription.GlobalStore[] sortedGlobalStores =
            globalStores.descendingSet().toArray(new GlobalStore[0]);
        int expectedId = 0;
        int subtopologiesIndex = sortedSubtopologies.Length - 1;
        int globalStoresIndex = sortedGlobalStores.Length - 1;
        while (subtopologiesIndex != -1 && globalStoresIndex != -1)
        {
            sb.Append("  ");
            TopologyDescription.Subtopology subtopology = sortedSubtopologies[subtopologiesIndex];
            TopologyDescription.GlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
            if (subtopology.id() == expectedId)
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
            TopologyDescription.Subtopology subtopology = sortedSubtopologies[subtopologiesIndex];
            sb.Append("  ");
            sb.Append(subtopology);
            subtopologiesIndex--;
        }
        while (globalStoresIndex != -1)
        {
            TopologyDescription.GlobalStore globalStore = sortedGlobalStores[globalStoresIndex];
            sb.Append("  ");
            sb.Append(globalStore);
            globalStoresIndex--;
        }
        return sb.ToString();
    }


    public bool Equals(object o)
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


    public int GetHashCode()
    {
        return Objects.hash(subtopologies, globalStores);
    }

}
