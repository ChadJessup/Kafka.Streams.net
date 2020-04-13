using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Clients;
using Kafka.Streams.Clients.Producers;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;

using System;
using System.Collections.Generic;

namespace Kafka.Streams.Tasks
{
    public class TaskCreator : AbstractTaskCreator<StreamTask>
    {
        private readonly ThreadCache? cache;
        private readonly IKafkaClientSupplier clientSupplier;
        private readonly IProducer<byte[], byte[]> threadProducer;

        public TaskCreator(
            KafkaStreamsContext context,
            ILogger<TaskCreator> logger,
            InternalTopologyBuilder builder,
            StreamsConfig config,
            StateDirectory stateDirectory,
            IChangelogReader storeChangelogReader,
            ThreadCache? cache,
            IKafkaClientSupplier clientSupplier,
            BaseProducer<byte[], byte[]> threadProducer)
        : base(
            context,
            logger,
            builder,
            config,
            stateDirectory,
            storeChangelogReader)
        {
            this.cache = cache;
            this.clientSupplier = clientSupplier;
            this.threadProducer = threadProducer;
        }

        public override StreamTask CreateTask(
            IConsumer<byte[], byte[]> consumer,
            TaskId taskId,
            string threadClientId,
            HashSet<TopicPartition> partitions)
        {
            return new StreamTask(
                this.Context,
                taskId,
                new List<TopicPartition>(partitions),
                this.builder.Build(taskId.topicGroupId),
                consumer,
                this.storeChangelogReader,
                this.config,
                this.stateDirectory,
                this.cache,
                new BasicProducerSupplier(
                    this.CreateProducer(taskId, threadClientId)));
        }

        public IProducer<byte[], byte[]> CreateProducer(
            TaskId id,
            string threadClientId)
        {
            // eos
            if (this.threadProducer == null)
            {
                var producerConfigs = this.config.GetProducerConfigs(this.GetTaskProducerClientId(threadClientId, id));

                this.logger.LogInformation($"Creating producer client for task {id}");
                producerConfigs.Set(StreamsConfig.TRANSACTIONAL_ID_CONFIG, $"{this.applicationId}-{id}");

                return this.clientSupplier.GetProducer(producerConfigs);
            }

            return this.threadProducer;
        }

        private string GetTaskProducerClientId(string threadClientId, TaskId taskId)
            => $"{threadClientId}-{taskId}-producer";

        public override void Close()
        {
            if (this.threadProducer != null)
            {
                try
                {
                    this.threadProducer.Dispose();
                }
                catch (Exception e)
                {
                    this.logger.LogError("Failed to Close producer due to the following error:", e);
                }
            }
        }
    }
}
