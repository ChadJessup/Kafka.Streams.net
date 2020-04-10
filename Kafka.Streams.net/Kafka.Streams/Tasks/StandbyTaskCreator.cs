using Confluent.Kafka;
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
            KafkaStreamsContext context,
            ILogger<StandbyTaskCreator> logger,
            InternalTopologyBuilder builder,
            StreamsConfig config,
            StateDirectory stateDirectory,
            IChangelogReader storeChangelogReader)
            : base(
                context,
                logger,
                builder,
                config,
                stateDirectory,
                storeChangelogReader)
        {
        }

        public override StandbyTask? CreateTask(
            IConsumer<byte[], byte[]> consumer,
            TaskId taskId,
            string threadClientId,
            HashSet<TopicPartition> partitions)
        {
            ProcessorTopology topology = this.builder.Build(taskId.topicGroupId);

            if (topology.StateStores.Any() && topology.StoreToChangelogTopic.Any())
            {
                return new StandbyTask(
                    this.Context,
                    this.Context.CreateLogger<StandbyTask>(),
                    taskId,
                    partitions.ToList(),
                    topology,
                    consumer,
                    this.storeChangelogReader,
                    this.config,
                    this.stateDirectory);
            }
            else
            {
                this.logger.LogTrace(
                    $"Skipped standby task {taskId} with assigned partitions {partitions} " +
                        "since it does not have any state stores to materialize");

                return null;
            }
        }
    }
}
