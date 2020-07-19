﻿using Confluent.Kafka;
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
            InternalTopologyBuilder builder,
            IChangelogReader storeChangelogReader,
            ThreadCache? cache,
            IKafkaClientSupplier clientSupplier,
            BaseProducer<byte[], byte[]> threadProducer)
        : base(
            context,
            builder,
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
            if (taskId is null)
            {
                throw new ArgumentNullException(nameof(taskId));
            }

            return new StreamTask(
                this.Context,
                taskId,
                new HashSet<TopicPartition>(partitions),
                this.Builder.Build(taskId.TopicGroupId),
                consumer,
                this.Config,
                this.StateDirectory,
                this.cache,
                null,
                //this.storeChangelogReader,
                //new BasicProducerSupplier(
                //    this.CreateProducer(taskId, threadClientId)),
                null);
        }

        public IProducer<byte[], byte[]> CreateProducer(
            TaskId id,
            string threadClientId)
        {
            // eos
            if (this.threadProducer == null)
            {
                var producerConfigs = this.Config.GetProducerConfigs(this.GetTaskProducerClientId(threadClientId, id));

                this.Logger.LogInformation($"Creating producer client for task {id}");
                producerConfigs.Set(StreamsConfig.TRANSACTIONAL_ID_CONFIGConfig, $"{this.ApplicationId}-{id}");

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
                    this.Logger.LogError("Failed to Close producer due to the following error:", e);
                }
            }
        }
    }
}
