﻿using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Tasks;

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

        public IProducer<byte[], byte[]> Get()
        {
            // eos
            if (this.threadProducer == null)
            {
                var producerConfigs = this.config.GetProducerConfigs(this.GetTaskProducerClientId(this.threadClientId, this.id));
                //    log.LogInformation("Creating producer client for task {}", id);
                producerConfigs.Set("transactional.id", $"{this.applicationId}-{this.id}");

                return this.clientSupplier.GetProducer(producerConfigs);
            }

            return this.threadProducer;
        }

        public string GetTaskProducerClientId(string threadClientId, TaskId taskId)
            => $"{threadClientId}-{taskId}-producer";
    }
}
