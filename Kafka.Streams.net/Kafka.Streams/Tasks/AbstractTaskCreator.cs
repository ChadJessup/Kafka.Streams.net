using Confluent.Kafka;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.Processors.Internals.Metrics;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace Kafka.Streams.Tasks
{
    public abstract class AbstractTaskCreator<T>
        where T : ITask
    {
        protected ITime time { get; }
        protected ILogger log { get; }
        protected StreamsMetricsImpl streamsMetrics { get; }

        public AbstractTaskCreator(
            InternalTopologyBuilder builder,
            StreamsConfig config,
            StreamsMetricsImpl streamsMetrics,
            StateDirectory stateDirectory,
            IChangelogReader storeChangelogReader,
            ITime time,
            ILogger log)
        {
            this.applicationId = config.Get(StreamsConfigPropertyNames.ApplicationId);
            this.builder = builder;
            this.config = config;
            this.streamsMetrics = streamsMetrics;
            this.stateDirectory = stateDirectory;
            this.storeChangelogReader = storeChangelogReader;
            this.time = time;
            this.log = log;
        }

        public string applicationId { get; }
        public InternalTopologyBuilder builder { get; }
        public StreamsConfig config { get; }
        public StateDirectory stateDirectory { get; }
        public IChangelogReader storeChangelogReader { get; }

        public List<T> createTasks(
            IConsumer<byte[], byte[]> consumer,
            Dictionary<TaskId, HashSet<TopicPartition>> tasksToBeCreated)
        {
            List<T> createdTasks = new List<T>();
            foreach (KeyValuePair<TaskId, HashSet<TopicPartition>> newTaskAndPartitions in tasksToBeCreated)
            {
                TaskId taskId = newTaskAndPartitions.Key;
                HashSet<TopicPartition> partitions = newTaskAndPartitions.Value;
                T task = createTask(consumer, taskId, partitions);
                if (task != null)
                {
                    log.LogTrace($"Created task {{{taskId}}} with assigned partitions {{{partitions}}}");

                    createdTasks.Add(task);
                }

            }

            return createdTasks;
        }

        public abstract T createTask(IConsumer<byte[], byte[]> consumer, TaskId id, HashSet<TopicPartition> partitions);

        public virtual void close() { }
    }
}