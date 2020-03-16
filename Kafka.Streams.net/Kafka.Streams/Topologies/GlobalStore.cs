using Kafka.Streams.Processors.Internals;
using System.Collections.Generic;

namespace Kafka.Streams.Topologies
{
    public class GlobalStore : IGlobalStore
    {
        public ISource source { get; }
        public IProcessor processor { get; }
        public int id { get; }

        public GlobalStore(
            string sourceName,
            string processorName,
            string storeName,
            string topicName,
            int id)
        {
            source = new Source(sourceName, new HashSet<string>() { topicName }, null);
            processor = new Processor(processorName, new HashSet<string>() { storeName });
            source.Successors.Add(processor);
            processor.Predecessors.Add(source);

            this.id = id;
        }

        public override string ToString()
        {
            return $"Sub-topology: {id} for global store (will not generate tasks)\n"
                    + $"    {source.ToString()}\n"
                    + $"    {processor.ToString()}\n";
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

            var that = (GlobalStore)o;

            return source.Equals(that.source)
                && processor.Equals(that.processor);
        }

        public override int GetHashCode()
        {
            return (source, processor).GetHashCode();
        }
    }
}
