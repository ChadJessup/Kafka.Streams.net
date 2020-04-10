using System.Collections.Generic;
using Kafka.Streams.Topologies;

namespace Kafka.Streams.Processors.Internals
{
    public class Processor : AbstractNode, IProcessor
    {
        public HashSet<string> stores { get; }

        public Processor(
            string Name,
            HashSet<string> stores)
            : base(Name)
        {
            this.stores = stores;
        }

        public override string ToString()
        {
            return "";
            //"IProcessor: " + Name + " (stores: " + stores + ")\n      -=> "
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

            var processor = (IProcessor)o;
            // omit successor to avoid infinite loops
            return this.Name.Equals(processor.Name)
                && this.stores.Equals(processor.stores)
                && this.Predecessors.Equals(processor.Predecessors);
        }


        public override int GetHashCode()
        {
            // omit successor as it might change and alter the hash code
            return (this.Name, this.stores).GetHashCode();
        }
    }
}

