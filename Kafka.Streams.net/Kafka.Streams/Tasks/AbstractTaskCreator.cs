using Confluent.Kafka;
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

        public List<T> CreateTasks(
            ILoggerFactory loggerFactory,
            IConsumer<byte[], byte[]> consumer,
            string threadTaskId,
            Dictionary<TaskId, HashSet<TopicPartition>> tasksToBeCreated)
        {
            var createdTasks = new List<T>();
            foreach (KeyValuePair<TaskId, HashSet<TopicPartition>> newTaskAndPartitions in tasksToBeCreated)
            {
                TaskId taskId = newTaskAndPartitions.Key;
                HashSet<TopicPartition> partitions = newTaskAndPartitions.Value;
                T task = CreateTask(loggerFactory, consumer, taskId, threadTaskId, partitions);
                if (task != null)
                {
                    logger.LogTrace($"Created task {{{taskId}}} with assigned partitions {{{partitions}}}");

                    createdTasks.Add(task);
                }

            }

            return createdTasks;
        }

        public abstract T CreateTask(
            ILoggerFactory loggerFactory,
            IConsumer<byte[], byte[]> consumer,
            TaskId id,
            string threadClientId,
            HashSet<TopicPartition> partitions);

        public virtual void Close() { }
    }
}