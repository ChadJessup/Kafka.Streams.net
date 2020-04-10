using Kafka.Common;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Kafka.Streams.Nodes
{
    public interface IProcessorNode
    {
        string Name { get; }
        HashSet<string> StateStores { get; }
        List<IProcessorNode> Children { get; }
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
            string Name,
            HashSet<string>? stateStores,
            IKeyValueProcessor? processor)
        {
            this.Name = Name;
            this.StateStores = stateStores ?? new HashSet<string>();
            this.Clock = clock;
            this.Children = new List<IProcessorNode>();
            this.ChildByName = new Dictionary<string, IProcessorNode>();
            this.processor = processor;
        }

        public virtual void Process<K, V>(K key, V value)
        {
            this.processor?.Process(key, value);
        }

        public virtual void Init(IInternalProcessorContext context)
        {
            try
            {
                if (this.processor != null)
                {
                    this.processor.Init(context);
                }
            }
            catch (Exception e)
            {
                throw new StreamsException($"failed to initialize processor {this.Name}", e);
            }
        }

        public string Name { get; }
        protected IClock Clock { get; }

        public HashSet<string> StateStores { get; protected set; } = new HashSet<string>();

        // TODO: 'children' can be removed when #forward() via index is removed
        public List<IProcessorNode> Children { get; }
        protected Dictionary<string, IProcessorNode> ChildByName { get; }
        public ITimestampExtractor? TimestampExtractor { get; protected set; }

        private readonly IKeyValueProcessor? processor;

        public void Punctuate(long timestamp, IPunctuator punctuator)
        {
            punctuator.Punctuate(timestamp);
        }

        /**
         * @return a string representation of this node, useful for debugging.
         */
        public override string ToString()
        {
            return this.ToString("");
        }

        /**
         * @return a string representation of this node starting with the given indent, useful for debugging.
         */
        public virtual string ToString(string indent)
        {
            var sb = new StringBuilder($"{indent}{this.Name}:\n");

            if (this.StateStores.Any())
            {
                sb.Append(indent)
                  .Append("\tstates:\t\t[")
                  .Append(string.Join(",", this.StateStores))
                  .Append("]\n");
            }

            return sb.ToString();
        }

        public void AddChild(IProcessorNode child)
        {
            this.Children.Add(child);
            this.ChildByName.Add(child.Name, child);
        }

        public virtual IProcessorNode GetChild(string childName)
        {
            return this.ChildByName[childName];
        }
    }

    public class ProcessorNode<K, V> : ProcessorNode, IProcessorNode<K, V>, IProcessorNode
    {
        private readonly IKeyValueProcessor? processor;

        public ProcessorNode(IClock clock, string Name)
            : this(clock, Name, null, null)
        {
        }

        public ProcessorNode(
            IClock clock,
            string Name,
            IKeyValueProcessor? processor,
            HashSet<string>? stateStores)
            : base(clock, Name, stateStores, processor)
        {
            this.processor = processor;
        }

        public override IProcessorNode GetChild(string childName)
        {
            return this.ChildByName[childName];
        }

        IProcessorNode<K, V> IProcessorNode<K, V>.GetChild(string childName)
        {
            return (IProcessorNode<K, V>)this.ChildByName[childName];
        }

        public void AddChild(IProcessorNode<K, V> child)
        {
            base.AddChild(child);
        }

        public void Close()
        {
            try
            {
                this.processor?.Close();
            }
            catch (Exception e)
            {
                throw new StreamsException($"failed to Close processor {this.Name}", e);
            }
        }

        public virtual void Process(K key, V value)
        {
            this.processor?.Process(key, value);
        }
    }

    public interface IProcessorNode<K, V> : IProcessorNode
    {
        void AddChild(IProcessorNode<K, V> child);
        new IProcessorNode<K, V> GetChild(string childName);
    }
}
