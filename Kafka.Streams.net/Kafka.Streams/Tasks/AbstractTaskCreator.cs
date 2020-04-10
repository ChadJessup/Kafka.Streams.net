using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace Kafka.Streams.Tasks
{
    public abstract class AbstractTaskCreator<T>
        where T : ITask
    {
        public KafkaStreamsContext Context { get; }
        protected ILogger<AbstractTaskCreator<T>> logger { get; }

        public AbstractTaskCreator(
            KafkaStreamsContext context,
            ILogger<AbstractTaskCreator<T>> logger,
            InternalTopologyBuilder builder,
            StreamsConfig config,
            StateDirectory stateDirectory,
            IChangelogReader storeChangelogReader)
        {
            this.Context = context;
            this.logger = logger;

            this.applicationId = config.ApplicationId;
            this.builder = builder;
            this.config = config;
            this.stateDirectory = stateDirectory;
            this.storeChangelogReader = storeChangelogReader;
        }

        public string applicationId { get; }
        public InternalTopologyBuilder builder { get; }
        public StreamsConfig config { get; }
        public StateDirectory stateDirectory { get; }
        public IChangelogReader storeChangelogReader { get; }

        public List<T> CreateTasks(
            IConsumer<byte[], byte[]> consumer,
            string threadTaskId,
            Dictionary<TaskId, HashSet<TopicPartition>> tasksToBeCreated)
        {
            var createdTasks = new List<T>();
            foreach (var newTaskAndPartitions in tasksToBeCreated)
            {
                TaskId taskId = newTaskAndPartitions.Key;
                HashSet<TopicPartition> partitions = newTaskAndPartitions.Value;
                T task = this.CreateTask(consumer, taskId, threadTaskId, partitions);

                if (task != null)
                {
                    this.logger.LogTrace($"Created task {{{taskId}}} with assigned partitions {{{partitions}}}");

                    createdTasks.Add(task);
                }
            }

            return createdTasks;
        }

        public abstract T CreateTask(
            IConsumer<byte[], byte[]> consumer,
            TaskId id,
            string threadClientId,
            HashSet<TopicPartition> partitions);

        public virtual void Close() { }
    }
}
