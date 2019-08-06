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

public static class Subtopology : TopologyDescription.Subtopology
{

    private int id;
    private HashSet<TopologyDescription.Node> nodes;

    public Subtopology(int id, HashSet<TopologyDescription.Node> nodes)
    {
        this.id = id;
        this.nodes = new TreeSet<>(NODE_COMPARATOR);
        this.nodes.AddAll(nodes);
    }


    public int id()
    {
        return id;
    }


    public HashSet<TopologyDescription.Node> nodes()
    {
        return Collections.unmodifiableSet(nodes);
    }

    // visible for testing
    IEnumerator<TopologyDescription.Node> nodesInOrder()
    {
        return nodes.iterator();
    }


    public string ToString()
    {
        return "Sub-topology: " + id + "\n" + nodesAsString() + "\n";
    }

    private string nodesAsString()
    {
        StringBuilder sb = new StringBuilder();
        foreach (TopologyDescription.Node node in nodes)
        {
            sb.Append("    ");
            sb.Append(node);
            sb.Append('\n');
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

        Subtopology that = (Subtopology)o;
        return id == that.id
            && nodes.Equals(that.nodes);
    }


    public int GetHashCode()
    {
        return Objects.hash(id, nodes);
    }
}
