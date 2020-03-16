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
using Kafka.Streams.Errors;
using System;
using System.Linq;

namespace Kafka.Streams.KStream.Internals.Graph
{









    public class GraphGraceSearchUtil
    {

        private GraphGraceSearchUtil() { }

        public static long findAndVerifyWindowGrace(StreamsGraphNode streamsGraphNode)
        {
            return findAndVerifyWindowGrace(streamsGraphNode, "");
        }

        private static long findAndVerifyWindowGrace(StreamsGraphNode streamsGraphNode, string chain)
        {
            // error base case: we traversed off the end of the graph without finding a window definition
            if (streamsGraphNode == null)
            {
                throw new TopologyException(
                    "Window close time is only defined for windowed computations. Got [" + chain + "]."
                );
            }
            // base case: return if this node defines a grace period.
            {
                //              long gracePeriod = extractGracePeriod(streamsGraphNode);
                //            if (gracePeriod != null)
                {
                    //              return gracePeriod;
                }
            }

            var newChain = chain.Equals("") ? streamsGraphNode.NodeName : streamsGraphNode.NodeName + "=>" + chain;

            if (!streamsGraphNode.ParentNodes.Any())
            {
                // error base case: we traversed to the end of the graph without finding a window definition
                throw new TopologyException(
                    "Window close time is only defined for windowed computations. Got [" + newChain + "]."
                );
            }

            // recursive case: all parents must define a grace period, and we use the max of our parents' graces.
            long inheritedGrace = -1;
            foreach (StreamsGraphNode parentNode in streamsGraphNode.ParentNodes)
            {
                var parentGrace = findAndVerifyWindowGrace(parentNode, newChain);
                inheritedGrace = Math.Max(inheritedGrace, parentGrace);
            }

            if (inheritedGrace == -1)
            {
                throw new InvalidOperationException(); // shouldn't happen, and it's not a legal grace period
            }

            return inheritedGrace;
        }
    }
}
