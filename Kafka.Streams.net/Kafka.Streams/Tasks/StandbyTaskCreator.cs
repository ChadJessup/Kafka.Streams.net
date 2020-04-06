﻿using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;

using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Tasks
{
    public class StandbyTaskCreator : AbstractTaskCreator<StandbyTask>
    {
        public StandbyTaskCreator(
            ILogger<StandbyTaskCreator> logger,
            ILoggerFactory loggerFactory,
            InternalTopologyBuilder builder,
            StreamsConfig config,
            StateDirectory stateDirectory,
            IChangelogReader storeChangelogReader,
            IClock clock)
            : base(
                loggerFactory.CreateLogger<AbstractTaskCreator<StandbyTask>>(),
                builder,
                config,
                stateDirectory,
                storeChangelogReader,
                clock)
        {
        }

        public override StandbyTask? CreateTask(
            ILoggerFactory loggerFactory,
            IConsumer<byte[], byte[]> consumer,
            TaskId taskId,
            string threadClientId,
            HashSet<TopicPartition> partitions)
        {
            ProcessorTopology topology = builder.Build(taskId.topicGroupId);

            if (topology.StateStores.Any() && topology.StoreToChangelogTopic.Any())
            {
                return new StandbyTask(
                    loggerFactory,
                    loggerFactory.CreateLogger<StandbyTask>(),
                    taskId,
                    partitions.ToList(),
                    topology,
                    consumer,
                    storeChangelogReader,
                    config,
                    stateDirectory);
            }
            else
            {

                logger.LogTrace(
                    "Skipped standby task {} with assigned partitions {} " +
                        "since it does not have any state stores to materialize",
                    taskId, partitions
                );
                return null;
            }
        }
    }
}
