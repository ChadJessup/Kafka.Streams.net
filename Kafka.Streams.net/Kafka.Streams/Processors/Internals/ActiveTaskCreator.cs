using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Kafka.Common;
using Kafka.Streams.Clients;
using Kafka.Streams.Configs;
using Kafka.Streams.Errors;
using Kafka.Streams.Kafka.Streams;
using Kafka.Streams.State;
using Kafka.Streams.State.Internals;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads.Stream;
using Kafka.Streams.Topologies;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Processors.Internals
{
    public class ActiveTaskCreator
    {
        private readonly KafkaStreamsContext context;
        private readonly InternalTopologyBuilder builder;
        private readonly StreamsConfig config;
        private readonly StateDirectory stateDirectory;
        private readonly IChangelogReader storeChangelogReader;
        private readonly ThreadCache cache;
        private readonly IKafkaClientSupplier clientSupplier;
        private readonly string threadId;
        private readonly ILogger<ActiveTaskCreator> log;
        private readonly Dictionary<TaskId, StreamsProducer> taskProducers;
        private readonly ProcessingMode processingMode;

        public ActiveTaskCreator(
            KafkaStreamsContext context,
            StateDirectory stateDirectory,
            IChangelogReader storeChangelogReader,
            ThreadCache cache,
            IKafkaClientSupplier clientSupplier,
            string threadId,
            Guid processId)
        {
            this.context = context ?? throw new ArgumentNullException(nameof(context));

            this.stateDirectory = stateDirectory;
            this.storeChangelogReader = storeChangelogReader;
            this.cache = cache;
            this.clientSupplier = clientSupplier;
            this.threadId = threadId;
            this.log = this.context.CreateLogger<ActiveTaskCreator>();

            this.processingMode = StreamThread.ProcessingMode(this.config);

            if (this.processingMode == ProcessingMode.EXACTLY_ONCE_ALPHA)
            {
                this.ThreadProducer = null;
                this.taskProducers = new Dictionary<TaskId, StreamsProducer>();
            }
            else
            { // non-eos and eos-beta
                this.log.LogInformation("Creating thread producer client");

                string threadIdPrefix = string.Format("stream-thread [%s] ", Thread.CurrentThread.Name);

                this.ThreadProducer = new StreamsProducer(
                    this.config,
                    threadId,
                    clientSupplier,
                    null,
                    processId);

                this.taskProducers = new Dictionary<TaskId, StreamsProducer>();
            }
        }
        public StreamsProducer? ThreadProducer { get; }

        public void ReInitializeThreadProducer()
        {
            this.ThreadProducer?.ResetProducer();
        }

        public StreamsProducer StreamsProducerForTask(TaskId taskId)
        {
            if (this.processingMode != ProcessingMode.EXACTLY_ONCE_ALPHA)
            {
                throw new InvalidOperationException("Producer per thread is used.");
            }

            StreamsProducer taskProducer = this.taskProducers[taskId];
            if (taskProducer == null)
            {
                throw new InvalidOperationException("Unknown TaskId: " + taskId);
            }
            return taskProducer;
        }

        public IEnumerable<ITask> CreateTasks(
            IConsumer<byte[], byte[]> consumer,
            Dictionary<TaskId, HashSet<TopicPartition>> tasksToBeCreated)
        {
            List<ITask> createdTasks = new List<ITask>();
            foreach (var newTaskAndPartitions in tasksToBeCreated)
            {
                TaskId taskId = newTaskAndPartitions.Key;
                var partitions = newTaskAndPartitions.Value;

                string threadIdPrefix = $"stream-thread [{Thread.CurrentThread.Name}] ";
                string logPrefix = threadIdPrefix + $"task [{taskId}] ";

                ProcessorTopology topology = this.builder.BuildSubtopology(taskId.TopicGroupId);

                ProcessorStateManager stateManager = new ProcessorStateManager(
                    this.context,
                    taskId,
                    TaskType.ACTIVE,
                    StreamThread.EosEnabled(this.config),
                    this.stateDirectory,
                    null, //this.storeChangelogReader,
                    topology.StoreToChangelogTopic,
                    partitions);

                StreamsProducer streamsProducer;
                if (this.processingMode == ProcessingMode.EXACTLY_ONCE_ALPHA)
                {
                    this.log.LogInformation("Creating producer client for task {}", taskId);
                    streamsProducer = new StreamsProducer(
                        this.config,
                        this.threadId,
                        this.clientSupplier,
                        taskId,
                        null);

                    this.taskProducers.Add(taskId, streamsProducer);
                }
                else
                {
                    streamsProducer = this.ThreadProducer;
                }

                RecordCollector recordCollector = new RecordCollector(
                    taskId,
                    streamsProducer,
                    this.config.DefaultProductionExceptionHandler);

                ITask task = new StreamTask(
                    this.context,
                    taskId,
                    partitions,
                    topology,
                    consumer,
                    this.config,
                    this.stateDirectory,
                    this.cache,
                    stateManager,
                    recordCollector);

                this.log.LogTrace("Created task {} with assigned partitions {}", taskId, partitions);
                createdTasks.Add(task);
                //createTaskSensor.record();
            }

            return createdTasks;
        }

        public void CloseThreadProducerIfNeeded()
        {
            if (this.ThreadProducer != null)
            {
                try
                {
                    this.ThreadProducer.Close();
                }
                catch (RuntimeException e)
                {
                    throw new StreamsException("Thread producer encounter error trying to close.", e);
                }
            }
        }

        public void CloseAndRemoveTaskProducerIfNeeded(TaskId id)
        {
            StreamsProducer taskProducer = this.taskProducers[id];
            this.taskProducers.Remove(id);

            if (taskProducer != null)
            {
                try
                {
                    taskProducer.Close();
                }
                catch (RuntimeException e)
                {
                    throw new StreamsException("[" + id + "] task producer encounter error trying to close.", e);
                }
            }
        }

        public HashSet<string> ProducerClientIds()
        {
            if (this.ThreadProducer != null)
            {
                return new HashSet<string>(new[] { StreamsBuilder.GetThreadProducerClientId(this.threadId) });
            }
            else
            {
                return this.taskProducers.Keys
                    .Select(t => StreamsBuilder.GetTaskProducerClientId(this.threadId, t))
                    .ToHashSet();
            }
        }
    }
}
