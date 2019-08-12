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
using Kafka.Streams.Interfaces;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    [Serializable]
    public class NodeComparator : IComparer<INode>
    {
        public int Compare(
            INode node1,
            INode node2)
        {
            if (node1.Equals(node2))
            {
                return 0;
            }

            int size1 = ((AbstractNode)node1).size;
            int size2 = ((AbstractNode)node2).size;

            // it is possible that two nodes have the same sub-tree size (think two nodes connected via state stores)
            // in this case default to processor name string
            if (size1 != size2)
            {
                return size2 - size1;
            }
            else
            {

                return node1.name.CompareTo(node2.name);
            }
        }
    }
}
