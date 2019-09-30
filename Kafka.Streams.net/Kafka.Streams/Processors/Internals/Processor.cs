using System.Collections.Generic;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Nodes;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.Processors.Internals
{
    public class Processor : AbstractNode, IProcessor
    {
        public HashSet<string> stores { get; }

        public Processor(
            string name,
            HashSet<string> stores)
            : base(name)
        {
            this.stores = stores;
        }

        public override string ToString()
        {
            return "";
            //"IProcessor: " + name + " (stores: " + stores + ")\n      -=> "
            //    + nodeNames(successors) + "\n      <-- " + nodeNames(predecessors);
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

            IProcessor processor = (IProcessor)o;
            // omit successor to avoid infinite loops
            return Name.Equals(processor.Name)
                && stores.Equals(processor.stores)
                && Predecessors.Equals(processor.Predecessors);
        }


        public override int GetHashCode()
        {
            // omit successor as it might change and alter the hash code
            return (Name, stores).GetHashCode();
        }
    }
}

