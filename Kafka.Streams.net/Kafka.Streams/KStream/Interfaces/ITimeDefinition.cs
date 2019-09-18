using Kafka.Streams.KStream.Internals.Suppress;
using Kafka.Streams.Processor.Interfaces;

namespace Kafka.Streams.KStream.Interfaces
{
    /**
     * This interface should never be instantiated outside of this.
     */
    public interface ITimeDefinition<K, V>
    {
        long time(IProcessorContext context, K key);

        TimeDefinitionType type();
    }
}