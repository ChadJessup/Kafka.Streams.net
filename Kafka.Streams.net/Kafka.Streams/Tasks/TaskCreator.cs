using Confluent.Kafka;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Clients;
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
        private readonly ThreadCache cache;
        private readonly IKafkaClientSupplier clientSupplier;
        private readonly string threadClientId;
        private readonly IProducer<byte[], byte[]> threadProducer;

        public TaskCreator(
            ILogger<TaskCreator> logger,
            InternalTopologyBuilder builder,
            StreamsConfig config,
            StateDirectory stateDirectory,
            IChangelogReader storeChangelogReader,
            ThreadCache cache,
            ITime time,
            IKafkaClientSupplier clientSupplier,
            IProducer<byte[], byte[]> threadProducer,
            string threadClientId)
        : base(
            logger,
            builder,
            config,
            stateDirectory,
            storeChangelogReader,
            time)
        {
            this.cache = cache;
            this.clientSupplier = clientSupplier;
            this.threadProducer = threadProducer;
            this.threadClientId = threadClientId;
        }

        public override StreamTask createTask(
            ILoggerFactory loggerFactory,
            IConsumer<byte[], byte[]> consumer,
            TaskId taskId,
            HashSet<TopicPartition> partitions)
        {
            return new StreamTask(
                taskId,
                new List<TopicPartition>(partitions),
                builder.build(taskId.topicGroupId),
                consumer,
                storeChangelogReader,
                config,
                stateDirectory,
                cache,
                time,
                new BasicProducerSupplier(CreateProducer(taskId)));
        }

        private IProducer<byte[], byte[]> CreateProducer(TaskId id)
        {
            // eos
            if (threadProducer == null)
            {
                var producerConfigs = config.getProducerConfigs(this.GetTaskProducerClientId(threadClientId, id));

                logger.LogInformation($"Creating producer client for task {id}");
                producerConfigs.Set(StreamsConfigPropertyNames.TRANSACTIONAL_ID_CONFIG, $"{applicationId}-{id}");

                return clientSupplier.getProducer(producerConfigs);
            }

            return threadProducer;
        }

        private string GetTaskProducerClientId(string threadClientId, TaskId taskId)
            => $"{threadClientId}-{taskId}-producer";

        public override void close()
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