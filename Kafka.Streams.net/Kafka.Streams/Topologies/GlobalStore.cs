using Kafka.Streams.Processors.Internals;
using System;
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
            this.source = new Source(sourceName, new HashSet<string>() { topicName }, null);
            this.processor = new Processor(processorName, new HashSet<string>() { storeName });
            this.source.Successors.Add(this.processor);
            this.processor.Predecessors.Add(this.source);

            this.id = id;
        }

        public override string ToString()
        {
            return $"Sub-topology: {this.id} for global store (will not generate tasks)\n"
                    + $"    {this.source.ToString()}\n"
                    + $"    {this.processor.ToString()}\n";
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            if (o == null || this.GetType() != o.GetType())
            {
                return false;
            }

            var that = (GlobalStore)o;

            return this.source.Equals(that.source)
                && this.processor.Equals(that.processor);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(
                this.source,
                this.processor);
        }
    }
}
