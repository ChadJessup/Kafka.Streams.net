using Confluent.Kafka;
using Kafka.Streams.Clients.Consumers;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.Clients
{
    /**
     * {@code IKafkaClientSupplier} can be used to provide custom Kafka clients to a {@link KafkaStreams} instance.
     *
     * @see KafkaStreams#KafkaStreams(Topology, java.util.Properties, IKafkaClientSupplier)
     */
    public interface IKafkaClientSupplier
    {
        /**
         * Create an {@link Admin} which is used for internal topic management.
         *
         * @param config Supplied by the {@link java.util.Properties} given to the {@link KafkaStreams}
         * @return an instance of {@link Admin}
         */
        IAdminClient GetAdminClient(AdminClientConfig config);
        IAdminClient GetAdminClient(StreamsConfig config);

        /**
         * Create a {@link Producer} which is used to write records to sink topics.
         *
         * @param config {@link StreamsConfig#getProducerConfigs(string) producer config} which is supplied by the
         *               {@link java.util.Properties} given to the {@link KafkaStreams} instance
         * @return an instance of Kafka producer
         */
        IProducer<byte[], byte[]> getProducer(ProducerConfig config);

        /**
         * Create a {@link Consumer} which is used to read records of source topics.
         *
         * @param config {@link StreamsConfig#getMainConsumerConfigs(string, string, int) consumer config} which is
         *               supplied by the {@link java.util.Properties} given to the {@link KafkaStreams} instance
         * @return an instance of Kafka consumer
         */
        IConsumer<byte[], byte[]> getConsumer(ConsumerConfig config, IConsumerRebalanceListener rebalanceListener);

        /**
         * Create a {@link Consumer} which is used to read records to restore {@link IStateStore}s.
         *
         * @param config {@link StreamsConfig#getRestoreConsumerConfigs(string) restore consumer config} which is supplied
         *               by the {@link java.util.Properties} given to the {@link KafkaStreams}
         * @return an instance of Kafka consumer
         */
        IConsumer<byte[], byte[]> GetRestoreConsumer(ConsumerConfig config);

        /**
         * Create a {@link Consumer} which is used to consume records for {@link GlobalKTable}.
         *
         * @param config {@link StreamsConfig#getGlobalConsumerConfigs(string) global consumer config} which is supplied
         *               by the {@link java.util.Properties} given to the {@link KafkaStreams}
         * @return an instance of Kafka consumer
         */
        GlobalConsumer GetGlobalConsumer();
    }
}