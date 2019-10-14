using Confluent.Kafka;
using Kafka.Common.Metrics;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Clients;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Processors.Internals.Metrics;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Threads.KafkaStream;
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
        private readonly Sensor createTaskSensor;

        public TaskCreator(InternalTopologyBuilder builder,
                    StreamsConfig config,
                    StreamsMetricsImpl streamsMetrics,
                    StateDirectory stateDirectory,
                    IChangelogReader storeChangelogReader,
                    ThreadCache cache,
                    ITime time,
                    IKafkaClientSupplier clientSupplier,
                    IProducer<byte[], byte[]> threadProducer,
                    string threadClientId,
                    ILogger log)
        : base(
            builder,
            config,
            streamsMetrics,
            stateDirectory,
            storeChangelogReader,
            time,
            log)
        {
            this.cache = cache;
            this.clientSupplier = clientSupplier;
            this.threadProducer = threadProducer;
            this.threadClientId = threadClientId;
            // createTaskSensor = ThreadMetrics.createTaskSensor(streamsMetrics);
        }

        public override StreamTask createTask(
            IConsumer<byte[], byte[]> consumer,
            TaskId taskId,
            HashSet<TopicPartition> partitions)
        {
            createTaskSensor.record();

            return new StreamTask(
                taskId,
                new List<TopicPartition>(partitions),
                builder.build(taskId.topicGroupId),
                consumer,
                storeChangelogReader,
                config,
                streamsMetrics,
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

                log.LogInformation($"Creating producer client for task {id}");
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
                    log.LogError("Failed to close producer due to the following error:", e);
                }
            }
        }
    }
}