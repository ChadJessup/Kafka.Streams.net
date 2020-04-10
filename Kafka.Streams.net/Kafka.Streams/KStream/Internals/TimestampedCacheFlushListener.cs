using Kafka.Streams.Nodes;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class TimestampedCacheFlushListener<K, V> : ICacheFlushListener<K, IValueAndTimestamp<V>>
    {
        private readonly IInternalProcessorContext context;
        private readonly IProcessorNode<K, V> myNode;

        public TimestampedCacheFlushListener(IProcessorContext context)
        {
            if (context is null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            this.context = (IInternalProcessorContext)context;
            this.myNode = (IProcessorNode<K, V>)this.context.GetCurrentNode();
        }

        public void Apply(
            K key,
            IValueAndTimestamp<V> newValue,
            IValueAndTimestamp<V> oldValue,
            long timestamp)
        {
            var prev = this.context.GetCurrentNode();
            this.context.SetCurrentNode(this.myNode);

            try
            {
                this.context.Forward(
                    key,
                    new Change<IValueAndTimestamp<V>>(newValue, oldValue),
                    To.All().WithTimestamp(newValue != null ? newValue.Timestamp : timestamp));
            }
            finally
            {
                this.context.SetCurrentNode(prev);
            }
        }
    }
}
