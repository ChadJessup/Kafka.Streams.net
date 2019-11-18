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
