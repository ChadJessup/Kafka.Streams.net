using Confluent.Kafka;
using Kafka.Common.Metrics;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Processors.Internals.Metrics;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Streams.Tasks
{
    public class StandbyTaskCreator : AbstractTaskCreator<StandbyTask>
    {
        private readonly Sensor createTaskSensor;

        public StandbyTaskCreator(
            InternalTopologyBuilder builder,
            StreamsConfig config,
            StreamsMetricsImpl streamsMetrics,
            StateDirectory stateDirectory,
            IChangelogReader storeChangelogReader,
            ITime time,
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
            //createTaskSensor = ThreadMetrics.createTaskSensor(streamsMetrics);
        }


        public override StandbyTask createTask(IConsumer<byte[], byte[]> consumer,
                               TaskId taskId,
                               HashSet<TopicPartition> partitions)
        {
            createTaskSensor.record();

            ProcessorTopology topology = builder.build(taskId.topicGroupId);

            if (topology.stateStores.Any() && topology.storeToChangelogTopic.Any())
            {
                return new StandbyTask(
                    taskId,
                    partitions.ToList(),
                    topology,
                    consumer,
                    storeChangelogReader,
                    config,
                    streamsMetrics,
                    stateDirectory);
            }
            else
            {

                log.LogTrace(
                    "Skipped standby task {} with assigned partitions {} " +
                        "since it does not have any state stores to materialize",
                    taskId, partitions
                );
                return null;
            }
        }
    }
}
