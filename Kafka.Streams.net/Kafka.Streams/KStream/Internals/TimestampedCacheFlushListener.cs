using Kafka.Streams.Nodes;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;

namespace Kafka.Streams.KStream.Internals
{
    public class TimestampedCacheFlushListener<K, V> : ICacheFlushListener<K, ValueAndTimestamp<V>>
    {
        private readonly IInternalProcessorContext context;
        private readonly ProcessorNode<K, V> myNode;

        public TimestampedCacheFlushListener(IProcessorContext context)
        {
            this.context = (IInternalProcessorContext)context;
            myNode = this.context.GetCurrentNode<K, V>();
        }

        public void apply(
            K key,
            ValueAndTimestamp<V> newValue,
            ValueAndTimestamp<V> oldValue,
            long timestamp)
        {
            var prev = context.GetCurrentNode<K, V>();
            context.setCurrentNode(myNode);
            try
            {
                //context.forward(
                //    key,
                //    new Change<ValueAndTimestamp<V>>(newValue, oldValue),
                //    To.all().withTimestamp(newValue != null ? newValue.timestamp : timestamp));
            }
            finally
            {
                context.setCurrentNode(prev);
            }
        }
    }
}