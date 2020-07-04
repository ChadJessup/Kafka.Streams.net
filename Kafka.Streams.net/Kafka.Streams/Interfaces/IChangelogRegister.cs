using Confluent.Kafka;
using Kafka.Streams.Processors.Internals;

namespace Kafka.Streams.Interfaces
{
    /**
     * See {@link StoreChangelogReader}.
     */
    public interface IChangelogRegister
    {
        /**
         * Register a state store for restoration.
         *
         * @param partition the state store's changelog partition for restoring
         * @param stateManager the state manager used for restoring (one per task)
         */
        void Register(TopicPartition partition, ProcessorStateManager stateManager);
    }
}
