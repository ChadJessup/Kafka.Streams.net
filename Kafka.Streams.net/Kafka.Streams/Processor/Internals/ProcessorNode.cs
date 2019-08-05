using Kafka.Common.Metrics;
using Kafka.Common.Utils;
using Kafka.Common.Utils.Interfaces;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Processor.Internals
{
    public class ProcessorNode<K, V>
    {
        // TODO: 'children' can be removed when #forward() via index is removed
        private List<ProcessorNode<K, V>> children;
        private Dictionary<string, ProcessorNode<K, V>> childByName;

        private NodeMetrics nodeMetrics;
        private Processor<K, V> processor;
        private string name;
        private ITime time;

        public HashSet<string> stateStores;

        public ProcessorNode(string name)
            : this(name, null, null)
        {
        }


        public ProcessorNode(
            string name,
            Processor<K, V> processor,
            HashSet<string> stateStores)
        {
            this.name = name;
            this.processor = processor;
            this.children = new List<ProcessorNode<K, V>>();
            this.childByName = new Dictionary<string, ProcessorNode<K, V>>();
            this.stateStores = stateStores;
            this.time = new SystemTime();
        }

        ProcessorNode<K, V> getChild(string childName)
        {
            return childByName[childName];
        }

        public void addChild(ProcessorNode<K, V> child)
        {
            children.Add(child);
            childByName.Add(child.name, child);
        }

        public void init(IInternalProcessorContext context)
        {
            try
            {

                nodeMetrics = new NodeMetrics(context.metrics(), name, context);
                long startNs = time.nanoseconds();
                if (processor != null)
                {
                    processor.init(context);
                }
                nodeMetrics.nodeCreationSensor.record(time.nanoseconds() - startNs);
            }
            catch (Exception e)
            {
                throw new StreamsException(string.Format("failed to initialize processor %s", name), e);
            }
        }

        public void close()
        {
            try
            {

                long startNs = time.nanoseconds();
                if (processor != null)
                {
                    processor.close();
                }
                nodeMetrics.nodeDestructionSensor.record(time.nanoseconds() - startNs);
                nodeMetrics.removeAllSensors();
            }
            catch (Exception e)
            {
                throw new StreamsException(string.Format("failed to close processor %s", name), e);
            }
        }


        public void process(K key, V value)
        {
            long startNs = time.nanoseconds();
            processor.process(key, value);
            nodeMetrics.nodeProcessTimeSensor.record(time.nanoseconds() - startNs);
        }

        public void punctuate(long timestamp, Punctuator punctuator)
        {
            long startNs = time.nanoseconds();
            punctuator.punctuate(timestamp);
            nodeMetrics.nodePunctuateTimeSensor.record(time.nanoseconds() - startNs);
        }

        /**
         * @return a string representation of this node, useful for debugging.
         */

        public override string ToString()
        {
            return ToString("");
        }

        /**
         * @return a string representation of this node starting with the given indent, useful for debugging.
         */
        public string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder(indent + name + ":\n");
            if (stateStores != null && !stateStores.isEmpty())
            {
                sb.Append(indent).Append("\tstates:\t\t[");
                foreach (string store in stateStores)
                {
                    sb.Append(store);
                    sb.Append(", ");
                }

                sb.Length -= 2;  // Remove the last comma
                sb.Append("]\n");
            }
            return sb.ToString();
        }

        Sensor sourceNodeForwardSensor()
        {
            return nodeMetrics.sourceNodeForwardSensor;
        }
    }
}