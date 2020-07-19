using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Streams.Configs;
using Kafka.Streams.Processors.Internals;
using Kafka.Streams.State;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Tasks
{
    public abstract class AbstractTaskCreator<T>
        where T : class, ITask
    {
        public KafkaStreamsContext Context { get; }
        protected ILogger<AbstractTaskCreator<T>> Logger { get; }

        public AbstractTaskCreator(
            KafkaStreamsContext context,
            InternalTopologyBuilder builder,
            IChangelogReader storeChangelogReader)
        {
            this.Context = context ?? throw new ArgumentNullException(nameof(context));
            this.Builder = builder ?? throw new ArgumentNullException(nameof(builder));
            this.StoreChangelogReader = storeChangelogReader ?? throw new ArgumentNullException(nameof(storeChangelogReader));

            this.Logger = this.Context.CreateLogger<AbstractTaskCreator<T>>();

            this.ApplicationId = this.Context.StreamsConfig.ApplicationId;
            this.StateDirectory = this.Context.StateDirectory;
            this.Config = this.Context.StreamsConfig;
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
            if (tasksToBeCreated is null)
            {
                throw new ArgumentNullException(nameof(tasksToBeCreated));
            }

            var createdTasks = new List<T>();
            foreach (var newTaskAndPartitions in tasksToBeCreated)
            {
                TaskId taskId = newTaskAndPartitions.Key;
                HashSet<TopicPartition> partitions = newTaskAndPartitions.Value;
                var task = this.CreateTask(consumer, taskId, threadTaskId, partitions);

                if (task != null)
                {
                    this.Logger.LogTrace($"Created task {{{taskId}}} with assigned partitions {{{partitions}}}");

                    createdTasks.Add(task);
                }
            }

            return createdTasks;
        }

        public abstract T? CreateTask(
            IConsumer<byte[], byte[]> consumer,
            TaskId id,
            string threadClientId,
            HashSet<TopicPartition> partitions);

        public virtual void Close() { }
    }
}
