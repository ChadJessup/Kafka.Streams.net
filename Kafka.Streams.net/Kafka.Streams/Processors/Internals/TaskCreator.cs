﻿using Confluent.Kafka;
using Kafka.Common;
using Kafka.Common.Interfaces;
using Kafka.Common.Metrics;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Errors;
using Kafka.Streams.Interfaces;
using Kafka.Streams.KStream.Interfaces;
using Kafka.Streams.Processor.Interfaces;
using Kafka.Streams.Processor.Internals.Metrics;
using Kafka.Streams.Processors;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Processor.Internals
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
                new BasicProducerSupplier(createProducer(taskId)));
        }

        private IProducer<byte[], byte[]> createProducer(TaskId id)
        {
            // eos
            if (threadProducer == null)
            {
                var producerConfigs = config.getProducerConfigs(StreamThread.getTaskProducerClientId(threadClientId, id));

                log.LogInformation($"Creating producer client for task {id}");
                producerConfigs.Add(StreamsConfigPropertyNames.TRANSACTIONAL_ID_CONFIG, $"{applicationId}-{id}");

                return clientSupplier.getProducer(producerConfigs);
            }

            return threadProducer;
        }

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