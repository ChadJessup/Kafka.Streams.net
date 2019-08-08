﻿/*
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
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class InternalTopologyBuilder
    {
        public abstract class AbstractNode : TopologyDescription.Node
        {

            string name;
            HashSet<TopologyDescription.Node> predecessors = new SortedSet<>(NODE_COMPARATOR);
            HashSet<TopologyDescription.Node> successors = new SortedSet<>(NODE_COMPARATOR);

            // size of the sub-topology rooted at this node, including the node itself
            int size;

            AbstractNode(string name)
            {
                name = name ?? throw new System.ArgumentNullException("name cannot be null", nameof(name));
                this.name = name;
                this.size = 1;
            }


            public string name()
            {
                return name;
            }


            public HashSet<TopologyDescription.Node> predecessors()
            {
                return Collections.unmodifiableSet(predecessors);
            }


            public HashSet<TopologyDescription.Node> successors()
            {
                return Collections.unmodifiableSet(successors);
            }

            public void addPredecessor(TopologyDescription.Node predecessor)
            {
                predecessors.Add(predecessor);
            }

            public void addSuccessor(TopologyDescription.Node successor)
            {
                successors.Add(successor);
            }
        }
            }
}
