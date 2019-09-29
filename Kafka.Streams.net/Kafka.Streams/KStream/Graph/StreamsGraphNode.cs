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

using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Topologies;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class StreamsGraphNode
    {
        private readonly HashSet<StreamsGraphNode> childNodes = new HashSet<StreamsGraphNode>();
        private bool valueChangingOperation;
        private bool mergeNode;

        public StreamsGraphNode(string nodeName)
        {
            this.NodeName = nodeName;
        }

        public HashSet<StreamsGraphNode> ParentNodes { get; } = new HashSet<StreamsGraphNode>();
        public string NodeName { get; }
        public int BuildPriority { get; private set; }
        public bool HasWrittenToTopology { get; private set; } = false;
        public bool IsKeyChangingOperation { get; internal set; }
        public bool IsValueChangingOperation { get; internal set; }
        public bool IsMergeNode { get; }

        public string[] ParentNodeNames()
        {
            string[] parentNames = new string[ParentNodes.Count];
            int index = 0;

            foreach (StreamsGraphNode parentNode in ParentNodes)
            {
                parentNames[index++] = parentNode.NodeName;
            }

            return parentNames;
        }

        public bool AllParentsWrittenToTopology()
        {
            foreach (StreamsGraphNode parentNode in ParentNodes)
            {
                if (!parentNode.HasWrittenToTopology)
                {
                    return false;
                }
            }

            return true;
        }

        public HashSet<StreamsGraphNode> Children()
        {
            return new HashSet<StreamsGraphNode>(childNodes);
        }

        public void ClearChildren()
        {
            foreach (StreamsGraphNode childNode in childNodes)
            {
                childNode.ParentNodes.Remove(this);
            }

            childNodes.Clear();
        }

        public bool RemoveChild(StreamsGraphNode child)
        {
            return childNodes.Remove(child) && child.ParentNodes.Remove(this);
        }

        public void AddChild(StreamsGraphNode childNode)
        {
            this.childNodes.Add(childNode);
            childNode.ParentNodes.Add(this);
        }

        public void SetMergeNode(bool mergeNode)
        {
            this.mergeNode = mergeNode;
        }

        public void SetValueChangingOperation(bool valueChangingOperation)
        {
            this.valueChangingOperation = valueChangingOperation;
        }

        public void SetBuildPriority(int buildPriority)
        {
            this.BuildPriority = buildPriority;
        }

        public virtual void WriteToTopology(InternalTopologyBuilder topologyBuilder)
        {
        }

        public void SetHasWrittenToTopology(bool hasWrittenToTopology)
        {
            this.HasWrittenToTopology = hasWrittenToTopology;
        }

        public override string ToString()
        {
            string[] parentNames = ParentNodeNames();

            return $"StreamsGraphNode{{" +
                   $"nodeName='{this.NodeName}'" +
                   $", buildPriority={this.BuildPriority}" +
                   $", hasWrittenToTopology={this.HasWrittenToTopology}" +
                   $", keyChangingOperation={this.IsKeyChangingOperation}" +
                   $", valueChangingOperation={valueChangingOperation}" +
                   $", mergeNode={mergeNode}" +
                   $", parentNodes={Arrays.ToString(parentNames)}}}";
        }
    }
}
