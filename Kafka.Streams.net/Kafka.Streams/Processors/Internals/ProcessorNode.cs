using Kafka.Common.Metrics;
using Kafka.Common.Utils;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Errors;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals.Metrics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Processor.Internals
{
    public class ProcessorNode
    {
        public ProcessorNode(string name, HashSet<string> stateStores)
        {
            this.name = name;
            this.stateStores = stateStores;
            this.time = new SystemTime();
            this.children = new List<ProcessorNode>();
            this.childByName = new Dictionary<string, ProcessorNode>();
        }

        public string name { get; }
        protected ITime time { get; }

        public HashSet<string> stateStores { get; protected set; } = new HashSet<string>();

        // TODO: 'children' can be removed when #forward() via index is removed
        public List<ProcessorNode> children { get; }
        protected Dictionary<string, ProcessorNode> childByName { get; }

        public void punctuate(long timestamp, Punctuator punctuator)
        {
            long startNs = time.nanoseconds();
            punctuator.punctuate(timestamp);
            //nodeMetrics.nodePunctuateTimeSensor.record(time.nanoseconds() - startNs);
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
        public virtual string ToString(string indent)
        {
            StringBuilder sb = new StringBuilder($"{indent}{name}:\n");

            if (this.stateStores.Any())
            {
                sb.Append(indent)
                  .Append("\tstates:\t\t[")
                  .Append(string.Join(",", this.stateStores))
                  .Append("]\n");
            }

            return sb.ToString();
        }
    }

    public class ProcessorNode<K, V> : ProcessorNode
    {
        public NodeMetrics<K, V> nodeMetrics { get; private set; }
        private readonly IProcessor<K, V> processor;

        public ProcessorNode(string name)
            : this(name, null, null)
        {
        }

        public ProcessorNode(
            string name,
            IProcessor<K, V> processor,
            HashSet<string> stateStores)
            : base(name, stateStores)
        {
            this.processor = processor;
        }

        public ProcessorNode<K, V> getChild(string childName)
        {
            return (ProcessorNode<K, V>)childByName[childName];
        }

        public void addChild(ProcessorNode<K, V> child)
        {
            children.Add(child);
            childByName.Add(child.name, child);
        }

        public virtual void init(IInternalProcessorContext context)
        {
            try
            {
                nodeMetrics = new NodeMetrics<K, V>((StreamsMetricsImpl)context.metrics, name, context);
                long startNs = time.nanoseconds();
                if (processor != null)
                {
                    processor.init(context);
                }

                //nodeMetrics.nodeCreationSensor.record(time.nanoseconds() - startNs);
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

                //nodeMetrics.nodeDestructionSensor.record(time.nanoseconds() - startNs);
                nodeMetrics.removeAllSensors();
            }
            catch (Exception e)
            {
                throw new StreamsException(string.Format("failed to close processor %s", name), e);
            }
        }


        public virtual void process(K key, V value)
        {
            long startNs = time.nanoseconds();
            processor.process(key, value);
            nodeMetrics.nodeProcessTimeSensor.record(time.nanoseconds() - startNs);
        }
    }
}