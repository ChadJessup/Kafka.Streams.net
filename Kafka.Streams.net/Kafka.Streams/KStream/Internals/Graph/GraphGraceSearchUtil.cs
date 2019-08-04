/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
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
namespace Kafka.streams.kstream.internals.graph;

import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.KStreamSessionWindowAggregate;
import org.apache.kafka.streams.kstream.internals.KStreamWindowAggregate;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public  class GraphGraceSearchUtil {
    private GraphGraceSearchUtil() {}

    public static long findAndVerifyWindowGrace( StreamsGraphNode streamsGraphNode)
{
        return findAndVerifyWindowGrace(streamsGraphNode, "");
    }

    private static long findAndVerifyWindowGrace( StreamsGraphNode streamsGraphNode,  string chain)
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
             Long gracePeriod = extractGracePeriod(streamsGraphNode);
            if (gracePeriod != null)
{
                return gracePeriod;
            }
        }

         string newChain = chain.Equals("") ? streamsGraphNode.nodeName() : streamsGraphNode.nodeName() + "->" + chain;

        if (streamsGraphNode.parentNodes().isEmpty())
{
            // error base case: we traversed to the end of the graph without finding a window definition
            throw new TopologyException(
                "Window close time is only defined for windowed computations. Got [" + newChain + "]."
            );
        }

        // recursive case: all parents must define a grace period, and we use the max of our parents' graces.
        long inheritedGrace = -1;
        foreach ( StreamsGraphNode parentNode in streamsGraphNode.parentNodes())
{
             long parentGrace = findAndVerifyWindowGrace(parentNode, newChain);
            inheritedGrace = Math.Max(inheritedGrace, parentGrace);
        }

        if (inheritedGrace == -1)
{
            throw new InvalidOperationException(); // shouldn't happen, and it's not a legal grace period
        }

        return inheritedGrace;
    }

    private static Long extractGracePeriod( StreamsGraphNode node)
{
        if (node is StatefulProcessorNode)
{
             ProcessorSupplier processorSupplier = ((StatefulProcessorNode) node).processorParameters().processorSupplier();
            if (processorSupplier is KStreamWindowAggregate)
{
                 KStreamWindowAggregate kStreamWindowAggregate = (KStreamWindowAggregate) processorSupplier;
                 Windows windows = kStreamWindowAggregate.windows();
                return windows.gracePeriodMs();
            } else if (processorSupplier is KStreamSessionWindowAggregate)
{
                 KStreamSessionWindowAggregate kStreamSessionWindowAggregate = (KStreamSessionWindowAggregate) processorSupplier;
                 SessionWindows windows = kStreamSessionWindowAggregate.windows();
                return windows.gracePeriodMs() + windows.inactivityGap();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }
}
