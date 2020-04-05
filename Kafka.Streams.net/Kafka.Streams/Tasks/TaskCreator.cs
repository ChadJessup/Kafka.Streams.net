using Confluent.Kafka;
using Kafka.Streams.Clients;
using Kafka.Streams.Clients.Producers;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using NodaTime;
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
            ILogger<TaskCreator> logger,
            InternalTopologyBuilder builder,
            StreamsConfig config,
            StateDirectory stateDirectory,
            IChangelogReader storeChangelogReader,
            ThreadCache? cache,
            IClock clock,
            IKafkaClientSupplier clientSupplier,
            BaseProducer<byte[], byte[]> threadProducer)
        : base(
            logger,
            builder,
            config,
            stateDirectory,
            storeChangelogReader,
            clock)
        {
            this.cache = cache;
            this.clientSupplier = clientSupplier;
            this.threadProducer = threadProducer;
        }

        public override StreamTask CreateTask(
            ILoggerFactory loggerFactory,
            IConsumer<byte[], byte[]> consumer,
            TaskId taskId,
            string threadClientId,
            HashSet<TopicPartition> partitions)
        {
            return new StreamTask(
                taskId,
                new List<TopicPartition>(partitions),
                builder.Build(taskId.topicGroupId),
                consumer,
                storeChangelogReader,
                config,
                stateDirectory,
                cache,
                clock,
                new BasicProducerSupplier(CreateProducer(taskId, threadClientId)));
        }

        public IProducer<byte[], byte[]> CreateProducer(TaskId id, string threadClientId)
        {
            // eos
            if (threadProducer == null)
            {
                var producerConfigs = config.GetProducerConfigs(this.GetTaskProducerClientId(threadClientId, id));

                logger.LogInformation($"Creating producer client for task {id}");
                producerConfigs.Set(StreamsConfigPropertyNames.TRANSACTIONAL_ID_CONFIG, $"{applicationId}-{id}");

                return clientSupplier.GetProducer(producerConfigs);
            }

            return threadProducer;
        }

        private string GetTaskProducerClientId(string threadClientId, TaskId taskId)
            => $"{threadClientId}-{taskId}-producer";

        public override void Close()
        {
            if (threadProducer != null)
            {
                try
                {
                    threadProducer.Dispose();
                }
                catch (Exception e)
                {
                    logger.LogError("Failed to close producer due to the following error:", e);
                }
            }
        }
    }
}