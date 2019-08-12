﻿//using Confluent.Kafka;
//using Kafka.Common.Utils.Interfaces;
//using Kafka.Streams.Processor.Internals.Metrics;
//using Microsoft.Extensions.Logging;
//using System.Collections.Generic;
//using System.Collections.ObjectModel;

//namespace Kafka.Streams.Processor.Internals
//{
//    public abstract class AbstractTaskCreator<T>
//        where T : ITask
//    {
//        public string applicationId { get; }
//        public InternalTopologyBuilder builder { get; }
//        public StreamsConfig config { get; }
//        StreamsMetricsImpl streamsMetrics;
//        public StateDirectory stateDirectory { get; }
//        public IChangelogReader storeChangelogReader { get; }
//        ITime time;
//        ILogger log;


//        public AbstractTaskCreator(InternalTopologyBuilder builder,
//                            StreamsConfig config,
//                            StreamsMetricsImpl streamsMetrics,
//                            StateDirectory stateDirectory,
//                            IChangelogReader storeChangelogReader,
//                            ITime time,
//                            ILogger log)
//        {
//            this.applicationId = config.Get(StreamsConfig.APPLICATION_ID_CONFIG);
//            this.builder = builder;
//            this.config = config;
//            this.streamsMetrics = streamsMetrics;
//            this.stateDirectory = stateDirectory;
//            this.storeChangelogReader = storeChangelogReader;
//            this.time = time;
//            this.log = log;
//        }


//        public List<T> createTasks(IConsumer<byte[], byte[]> consumer,
//                                  Dictionary<TaskId, HashSet<TopicPartition>> tasksToBeCreated)
//        {
//            List<T> createdTasks = new List<T>();
//            foreach (KeyValuePair<TaskId, HashSet<TopicPartition>> newTaskAndPartitions in tasksToBeCreated)
//            {
//                TaskId taskId = newTaskAndPartitions.Key;
//                HashSet<TopicPartition> partitions = newTaskAndPartitions.Value;
//                T task = createTask(consumer, taskId, partitions);
//                if (task != null)
//                {
//                    log.LogTrace("Created task {} with assigned partitions {}", taskId, partitions);
//                    createdTasks.Add(task);
//                }

//            }

//            return createdTasks;
//        }

//        public abstract T createTask(IConsumer<byte[], byte[]> consumer, TaskId id, HashSet<TopicPartition> partitions);

//        public void close() { }
//    }
//}