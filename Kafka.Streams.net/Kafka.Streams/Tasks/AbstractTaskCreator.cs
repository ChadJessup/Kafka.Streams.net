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
        protected ILogger<AbstractTaskCreator<T>> Logger { get; }

        public AbstractTaskCreator(
            KafkaStreamsContext context,
            ILogger<AbstractTaskCreator<T>> logger,
            InternalTopologyBuilder builder,
            StreamsConfig config,
            StateDirectory stateDirectory,
            IChangelogReader storeChangelogReader)
        {
            this.Context = context;
            this.Logger = logger;

            this.ApplicationId = config.ApplicationId;
            this.Builder = builder;
            this.Config = config;
            this.StateDirectory = stateDirectory;
            this.StoreChangelogReader = storeChangelogReader;
        }

        public string ApplicationId { get; }
        public InternalTopologyBuilder Builder { get; }
        public StreamsConfig Config { get; }
        public StateDirectory StateDirectory { get; }
        public IChangelogReader StoreChangelogReader { get; }

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
                    this.Logger.LogTrace($"Created task {{{taskId}}} with assigned partitions {{{partitions}}}");

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
