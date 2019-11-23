using Kafka.Streams.Topologies;
using System.Collections.Generic;

namespace Kafka.Streams.KStream.Internals.Graph
{
    public interface IStreamsGraphNode
    {
        HashSet<IStreamsGraphNode> ParentNodes { get; }
        string NodeName { get; }
        int BuildPriority { get; }
        bool HasWrittenToTopology { get; }
        bool IsKeyChangingOperation { get; }
        bool IsValueChangingOperation { get; }
        bool IsMergeNode { get; }
        string[] ParentNodeNames();
        bool AllParentsWrittenToTopology();
        HashSet<IStreamsGraphNode> Children();
        void ClearChildren();
        bool RemoveChild(IStreamsGraphNode child);
        void AddChild(IStreamsGraphNode childNode);
        void SetMergeNode(bool mergeNode);
        void SetValueChangingOperation(bool valueChangingOperation);
        void SetBuildPriority(int buildPriority);
        void WriteToTopology(InternalTopologyBuilder topologyBuilder);
        void SetHasWrittenToTopology(bool hasWrittenToTopology);
    }
}