using Kafka.Streams.Topologies;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public class StreamsGraphNode : IStreamsGraphNode
    {
        private readonly HashSet<IStreamsGraphNode> childNodes = new HashSet<IStreamsGraphNode>();
        private bool valueChangingOperation;
        private bool mergeNode;

        public StreamsGraphNode(string nodeName)
        {
            this.NodeName = nodeName;
        }

        public HashSet<IStreamsGraphNode> ParentNodes { get; } = new HashSet<IStreamsGraphNode>();
        public string NodeName { get; }
        public int BuildPriority { get; private set; }
        public bool HasWrittenToTopology { get; private set; } = false;
        public bool IsKeyChangingOperation { get; internal set; }
        public bool IsValueChangingOperation { get; internal set; }
        public bool IsMergeNode { get; }

        public string[] ParentNodeNames()
        {
            var parentNames = new string[ParentNodes.Count];
            var index = 0;

            foreach (var parentNode in ParentNodes)
            {
                parentNames[index++] = parentNode.NodeName;
            }

            return parentNames;
        }

        public bool AllParentsWrittenToTopology()
        {
            foreach (var parentNode in ParentNodes)
            {
                if (!parentNode.HasWrittenToTopology)
                {
                    return false;
                }
            }

            return true;
        }

        public HashSet<IStreamsGraphNode> Children()
        {
            return new HashSet<IStreamsGraphNode>(childNodes);
        }

        public void ClearChildren()
        {
            foreach (var childNode in childNodes)
            {
                childNode.ParentNodes.Remove(this);
            }

            childNodes.Clear();
        }

        public bool RemoveChild(IStreamsGraphNode child)
        {
            return childNodes.Remove(child) && (child?.ParentNodes.Remove(this) ?? false);
        }

        public void AddChild(IStreamsGraphNode childNode)
        {
            this.childNodes.Add(childNode);
            childNode?.ParentNodes.Add(this);
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
            var parentNames = ParentNodeNames();

            return $"StreamsGraphNode{{" +
                   $"nodeName='{this.NodeName}'" +
                   $", buildPriority={this.BuildPriority}" +
                   $", hasWrittenToTopology={this.HasWrittenToTopology}" +
                   $", keyChangingOperation={this.IsKeyChangingOperation}" +
                   $", valueChangingOperation={valueChangingOperation}" +
                   $", mergeNode={mergeNode}" +
                   $", parentNodes={string.Join(',', parentNames)}}}";
        }
    }
}
