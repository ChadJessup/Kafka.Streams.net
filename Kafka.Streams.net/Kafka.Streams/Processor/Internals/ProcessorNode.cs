using Kafka.Common.Metrics;
using Kafka.Common.Utils;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Errors;
using Kafka.Streams.IProcessor.Interfaces;
using Kafka.Streams.IProcessor.Internals.Metrics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Streams.IProcessor.Internals
{
    public class ProcessorNode<K, V>
    {
        // TODO: 'children' can be removed when #forward() via index is removed
        public List<ProcessorNode<K, V>> children { get; }
        private Dictionary<string, ProcessorNode<K, V>> childByName;

        public NodeMetrics<K, V> nodeMetrics { get; private set; }
        private IProcessor<K, V> processor;
        public string name { get; }
        private ITime time;

        public HashSet<string> stateStores;

        public ProcessorNode(string name)
            : this(name, null, null)
        {
        }


        public ProcessorNode(
            string name,
            IProcessor<K, V> processor,
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

        public void init(IInternalProcessorContext<K, V> context)
        {
            try
            {
                nodeMetrics = new NodeMetrics<K, V>((StreamsMetricsImpl)context.metrics, name, context);
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
            if (stateStores != null && stateStores.Any())
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
    }
}