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
using Kafka.Streams.Processor.Interfaces;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Processor.Internals
{


    public class ProcessorTopology {

        private List<ProcessorNode> processorNodes;
        private Dictionary<string, SourceNode> sourcesByTopic;
        private Dictionary<string, SinkNode> sinksByTopic;
        private List<IStateStore> stateStores;
        private List<IStateStore> globalStateStores;
        private Dictionary<string, string> storeToChangelogTopic;
        private Set<string> repartitionTopics;

        public ProcessorTopology(List<ProcessorNode> processorNodes,
                                 Dictionary<string, SourceNode> sourcesByTopic,
                                 Dictionary<string, SinkNode> sinksByTopic,
                                 List<IStateStore> stateStores,
                                 List<IStateStore> globalStateStores,
                                 Dictionary<string, string> storeToChangelogTopic,
                                 Set<string> repartitionTopics)
{
            this.processorNodes = Collections.unmodifiableList(processorNodes);
            this.sourcesByTopic = Collections.unmodifiableMap(sourcesByTopic);
            this.sinksByTopic = Collections.unmodifiableMap(sinksByTopic);
            this.stateStores = Collections.unmodifiableList(stateStores);
            this.globalStateStores = Collections.unmodifiableList(globalStateStores);
            this.storeToChangelogTopic = Collections.unmodifiableMap(storeToChangelogTopic);
            this.repartitionTopics = Collections.unmodifiableSet(repartitionTopics);
        }

        public Set<string> sourceTopics()
{
            return sourcesByTopic.keySet();
        }

        public SourceNode source(string topic)
{
            return sourcesByTopic[topic];
        }

        public Set<SourceNode> sources()
{
            return new HashSet<>(sourcesByTopic.values());
        }

        public Set<string> sinkTopics()
{
            return sinksByTopic.keySet();
        }

        public SinkNode sink(string topic)
{
            return sinksByTopic[topic];
        }

        public List<ProcessorNode> processors()
{
            return processorNodes;
        }

        bool isRepartitionTopic(string topic)
        {
            return repartitionTopics.contains(topic);
        }

        public bool hasPersistentLocalStore()
        {
            foreach (IStateStore store in stateStores())
            {
                if (store.persistent())
{
                    return true;
                }
            }

            return false;
        }

        public bool hasPersistentGlobalStore()
        {
            foreach (IStateStore store in globalStateStores())
            {
                if (store.persistent())
                {
                    return true;
                }
            }

            return false;
        }

        private string childrenToString(string indent, List<ProcessorNode<K, V>> children)
        {
            if (children == null || !children.Any())
            {
                return "";
            }

            StringBuilder sb = new StringBuilder(indent + "\tchildren:\t["];
            foreach (var child in children)
{
                sb.Append(child.name);
                sb.Append(", ");
            }
            sb.Length -= 2;  // Remove the last comma
            sb.Append("]\n");

            // recursively print children
            foreach (ProcessorNode <?, ?> child in children)
{
                sb.Append(child.ToString(indent)).Append(childrenToString(indent, child.children()));
            }
            return sb.ToString();
        }

        /**
         * Produces a string representation containing useful information this topology starting with the given indent.
         * This is useful in debugging scenarios.
         * @return A string representation of this instance.
         */

        public override string ToString()
{
            return ToString("");
        }

        /**
         * Produces a string representation containing useful information this topology.
         * This is useful in debugging scenarios.
         * @return A string representation of this instance.
         */
        public string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder(indent + "ProcessorTopology:\n");

            // start from sources
            foreach (var source in sourcesByTopic.values())
{
                sb.Append(source.ToString(indent + "\t")).Append(childrenToString(indent + "\t", source.children()));
            }

            return sb.ToString();
        }

        // for testing only
        public HashSet<string> processorConnectedStateStores(string processorName)
{
            foreach (ProcessorNode <?, ?> node in processorNodes)
{
                if (node.name().Equals(processorName))
{
                    return node.stateStores;
                }
            }

            return Collections.emptySet();
        }
    }
}