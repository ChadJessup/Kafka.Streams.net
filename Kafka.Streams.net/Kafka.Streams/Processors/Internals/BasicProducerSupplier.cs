﻿using Confluent.Kafka;
using Kafka.Streams.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using System.Collections.Generic;

namespace Kafka.Streams.Processor.Internals
{
    public class BasicProducerSupplier : IProducerSupplier
    {
        private readonly TaskId id;
        private readonly StreamsConfig config;
        private readonly string threadClientId;
        private readonly IProducer<byte[], byte[]> threadProducer;
        private readonly string applicationId;
        private readonly IKafkaClientSupplier clientSupplier;

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
                Dictionary<string, object> producerConfigs = config.getProducerConfigs(StreamThread.getTaskProducerClientId(threadClientId, id));
                //    log.LogInformation("Creating producer client for task {}", id);
                producerConfigs.Add("transactional.id", applicationId + "-" + id);
                return clientSupplier.getProducer(producerConfigs);
            }

            return threadProducer;
        }
    }
}
