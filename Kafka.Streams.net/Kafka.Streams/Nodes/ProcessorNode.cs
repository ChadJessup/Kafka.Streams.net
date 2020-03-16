using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using NodaTime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Nodes
{
    public interface IProcessorNode
    {
        string Name { get; }
        HashSet<string> stateStores { get; }
        List<IProcessorNode> children { get; }
        void AddChild(IProcessorNode child);
        IProcessorNode GetChild(string childName);
        string ToString(string indent);
        ITimestampExtractor? TimestampExtractor { get; }
        void Punctuate(long timestamp, IPunctuator punctuator);
        void Process<K, V>(K key, V value);
    }

    public class ProcessorNode : IProcessorNode
    {
        public ProcessorNode(
            IClock clock,
            string name,
            HashSet<string>? stateStores,
            IKeyValueProcessor? processor)
        {
            this.Name = name;
            this.stateStores = stateStores ?? new HashSet<string>();
            this.clock = clock;
            this.children = new List<IProcessorNode>();
            this.childByName = new Dictionary<string, IProcessorNode>();
            this.processor = processor;
        }

        public virtual void Process<K, V>(K key, V value)
        {
            processor?.Process(key, value);
        }

        public virtual void Init(IInternalProcessorContext context)
        {
            try
            {
                if (processor != null)
                {
                    processor.Init(context);
                }
            }
            catch (Exception e)
            {
                throw new StreamsException($"failed to initialize processor {Name}", e);
            }
        }

        public string Name { get; }
        protected IClock clock { get; }

        public HashSet<string> stateStores { get; protected set; } = new HashSet<string>();

        // TODO: 'children' can be removed when #forward() via index is removed
        public List<IProcessorNode> children { get; }
        protected Dictionary<string, IProcessorNode> childByName { get; }
        public ITimestampExtractor? TimestampExtractor { get; }

        private readonly IKeyValueProcessor? processor;

        public void Punctuate(long timestamp, IPunctuator punctuator)
        {
            punctuator.punctuate(timestamp);
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
            var sb = new StringBuilder($"{indent}{Name}:\n");

            if (this.stateStores.Any())
            {
                sb.Append(indent)
                  .Append("\tstates:\t\t[")
                  .Append(string.Join(",", this.stateStores))
                  .Append("]\n");
            }

            return sb.ToString();
        }

        public void AddChild(IProcessorNode child)
        {
            children.Add(child);
            childByName.Add(child.Name, child);
        }

        public IProcessorNode GetChild(string childName)
        {
            return this.childByName[childName];
        }
    }

    public class ProcessorNode<K, V> : ProcessorNode, IProcessorNode<K, V>, IProcessorNode
    {
        private readonly IKeyValueProcessor<K, V>? processor;

        public ProcessorNode(IClock clock, string name)
            : this(clock, name, null, null)
        {
        }

        public ProcessorNode(
            IClock clock,
            string name,
            IKeyValueProcessor<K, V>? processor,
            HashSet<string>? stateStores)
            : base(clock, name, stateStores, processor)
        {
            this.processor = processor;
        }

        public IProcessorNode<K, V> GetChild(string childName)
        {
            return (IProcessorNode<K, V>)childByName[childName];
        }

        public void AddChild(IProcessorNode<K, V> child)
        {
            base.AddChild(child);
        }

        public void close()
        {
            try
            {
                processor?.Close();
            }
            catch (Exception e)
            {
                throw new StreamsException($"failed to close processor {Name}", e);
            }
        }

        public virtual void Process(K key, V value)
        {
            processor?.Process(key, value);
        }
    }

    public interface IProcessorNode<K, V> : IProcessorNode
    {
        void AddChild(IProcessorNode<K, V> child);
        IProcessorNode<K, V> GetChild(string childName);
    }
}
