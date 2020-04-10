using Kafka.Streams.Errors;
using System;
using System.Linq;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public static class GraphGraceSearchUtil
    {
        public static long FindAndVerifyWindowGrace(StreamsGraphNode streamsGraphNode)
        {
            return FindAndVerifyWindowGrace(streamsGraphNode, "");
        }

        private static long FindAndVerifyWindowGrace(StreamsGraphNode streamsGraphNode, string chain)
        {
            // error base case: we traversed off the end of the graph without finding a window definition
            if (streamsGraphNode == null)
            {
                throw new TopologyException(
                    "Window Close time is only defined for windowed computations. Got [" + chain + "]."
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
                    "Window Close time is only defined for windowed computations. Got [" + newChain + "]."
                );
            }

            // recursive case: All parents must define a grace period, and we use the max of our parents' graces.
            long inheritedGrace = -1;
            foreach (StreamsGraphNode parentNode in streamsGraphNode.ParentNodes)
            {
                var parentGrace = FindAndVerifyWindowGrace(parentNode, newChain);
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
