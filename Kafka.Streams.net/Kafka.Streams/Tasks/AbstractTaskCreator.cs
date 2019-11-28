using Confluent.Kafka;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using NodaTime;
using System.Collections.Generic;

namespace Kafka.Streams.Tasks
{
    public abstract class AbstractTaskCreator<T>
        where T : ITask
    {
        protected IClock clock { get; }
        protected ILogger<AbstractTaskCreator<T>> logger { get; }

        public AbstractTaskCreator(
            ILogger<AbstractTaskCreator<T>> logger,
            InternalTopologyBuilder builder,
            StreamsConfig config,
            StateDirectory stateDirectory,
            IChangelogReader storeChangelogReader,
            IClock clock)
        {
            this.logger = logger;

            this.applicationId = config.ApplicationId;
            this.builder = builder;
            this.config = config;
            this.stateDirectory = stateDirectory;
            this.storeChangelogReader = storeChangelogReader;
            this.clock = clock;
        }

        public string applicationId { get; }
        public InternalTopologyBuilder builder { get; }
        public StreamsConfig config { get; }
        public StateDirectory stateDirectory { get; }
        public IChangelogReader storeChangelogReader { get; }

        public List<T> createTasks(
            ILoggerFactory loggerFactory,
            IConsumer<byte[], byte[]> consumer,
            Dictionary<TaskId, HashSet<TopicPartition>> tasksToBeCreated)
        {
            List<T> createdTasks = new List<T>();
            foreach (KeyValuePair<TaskId, HashSet<TopicPartition>> newTaskAndPartitions in tasksToBeCreated)
            {
                TaskId taskId = newTaskAndPartitions.Key;
                HashSet<TopicPartition> partitions = newTaskAndPartitions.Value;
                T task = createTask(loggerFactory, consumer, taskId, partitions);
                if (task != null)
                {
                    logger.LogTrace($"Created task {{{taskId}}} with assigned partitions {{{partitions}}}");

                    createdTasks.Add(task);
                }

            }

            return createdTasks;
        }

        public abstract T createTask(
            ILoggerFactory loggerFactory,
            IConsumer<byte[], byte[]> consumer,
            TaskId id,
            HashSet<TopicPartition> partitions);

        public virtual void close() { }
    }
}