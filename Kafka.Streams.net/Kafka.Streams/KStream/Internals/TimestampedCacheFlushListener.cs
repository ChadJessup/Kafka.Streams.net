using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using System;

namespace Kafka.Streams.KStream.Internals
{
    public class TimestampedCacheFlushListener<K, V> : ICacheFlushListener<K, ValueAndTimestamp<V>>
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
            myNode = (IProcessorNode<K, V>)this.context.GetCurrentNode();
        }

        public void Apply(
            K key,
            ValueAndTimestamp<V> newValue,
            ValueAndTimestamp<V> oldValue,
            long timestamp)
        {
            var prev = context.GetCurrentNode();
            context.SetCurrentNode(myNode);
            try
            {
                //context.Forward(
                //    key,
                //    new Change<ValueAndTimestamp<V>>(newValue, oldValue),
                //    To.All().WithTimestamp(newValue != null ? newValue.timestamp : timestamp));
            }
            finally
            {
                context.SetCurrentNode(prev);
            }
        }
    }
}