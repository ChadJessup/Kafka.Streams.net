using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads.KafkaStream;

namespace Kafka.Streams.Clients
{
    public class BasicProducerSupplier : IProducerSupplier
    {
        private readonly TaskId id;
        private readonly StreamsConfig config;
        private readonly string threadClientId;
        private readonly IProducer<byte[], byte[]> threadProducer;
        private readonly string applicationId;
        private readonly IKafkaClientSupplier clientSupplier;
        private readonly IProducer<byte[], byte[]> producer;

        public BasicProducerSupplier(IProducer<byte[], byte[]> producer)
        {
            this.producer = producer;
        }

        public BasicProducerSupplier(
            TaskId id,
            string threadClientId,
            IProducer<byte[], byte[]> threadProducer,
            StreamsConfig config,
            string applicationId,
            IKafkaClientSupplier clientSupplier)
        {
            this.id = id;
            this.config = config;
            this.threadClientId = threadClientId;
            this.threadProducer = threadProducer;
            this.applicationId = applicationId;
            this.clientSupplier = clientSupplier;
        }

        public IProducer<byte[], byte[]> get()
        {
            // eos
            if (threadProducer == null)
            {
                var producerConfigs = config.getProducerConfigs(KafkaStreamThread.getTaskProducerClientId(threadClientId, id));
                //    log.LogInformation("Creating producer client for task {}", id);
                producerConfigs.Add("transactional.id", applicationId + "-" + id);

                return clientSupplier.getProducer(producerConfigs);
            }

            return threadProducer;
        }
    }
}
