using Kafka.Streams.Interfaces;
using Kafka.Streams.Nodes;
using System.Collections.Generic;

namespace Kafka.Streams.State
{
    public class GlobalStore : IGlobalStore
    {
        public ISourceNode source { get; }
        public IProcessor processor { get; }
        public int id { get; }

        public GlobalStore(
            string sourceName,
            string processorName,
            string storeName,
            string topicName,
            int id)
        {
            source = new SourceNode(sourceName, new HashSet<string>() { topicName }, null);
            processor = new Processor(processorName, new HashSet<string>() { storeName });
            source.successors.Add(processor);
            processor.predecessors.Add(source);

            this.id = id;
        }

        public override string ToString()
        {
            return "Sub-topology: " + id + " for global store (will not generate tasks)\n"
                    + "    " + source.ToString() + "\n"
                    + "    " + processor.ToString() + "\n";
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            GlobalStore that = (GlobalStore)o;
            return source.Equals(that.source)
                && processor.Equals(that.processor);
        }

        public override int GetHashCode()
        {
            return (source, processor).GetHashCode();
        }
    }
}
