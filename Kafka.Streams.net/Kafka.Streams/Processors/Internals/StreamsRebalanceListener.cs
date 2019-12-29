using Confluent.Kafka;
using Kafka.Common.Utils.Interfaces;
using Kafka.Streams.Extensions;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads.KafkaStream;
using Microsoft.Extensions.Logging;
using NodaTime;
using System;
using System.Collections.Generic;

namespace Kafka.Streams.Processors.Internals
{
    public class StreamsRebalanceListener : IConsumerRebalanceListener
    {
        private readonly IClock clock;
        private readonly TaskManager taskManager;
        private readonly KafkaStreamThread streamThread;
        private readonly ILogger log;

        public StreamsRebalanceListener(
            IClock clock,
            TaskManager taskManager,
            KafkaStreamThread streamThread,
            ILogger log)
        {
            this.clock = clock;
            this.taskManager = taskManager;
            this.streamThread = streamThread;
            this.log = log;
        }

        public void OnPartitionsAssigned(IConsumer<byte[], byte[]> consumer, List<TopicPartition> assignedPartitions)
        {
            log.LogDebug(
                $"at state {streamThread.State}: partitions {assignedPartitions.ToJoinedString()} assigned at the end of consumer rebalance.\n" +
                $"\tcurrent suspended active tasks: {taskManager.SuspendedActiveTaskIds().ToJoinedString()}\n" +
                $"\tcurrent suspended standby tasks: {taskManager.SuspendedStandbyTaskIds().ToJoinedString()}\n");

            if (streamThread.AssignmentErrorCode == (int)StreamsPartitionAssignor.Error.INCOMPLETE_SOURCE_TOPIC_METADATA)
            {
                log.LogError($"Received error code {streamThread.AssignmentErrorCode} - shutdown");
                streamThread.Shutdown();

                return;
            }

            long start = clock.GetCurrentInstant().ToUnixTimeMilliseconds();
            try
            {
                if (!streamThread.State.SetState(KafkaStreamThreadStates.PARTITIONS_ASSIGNED))
                {
                    log.LogDebug($"Skipping task creation in rebalance because we are already in {streamThread.State} state.");
                }
                else if (streamThread.AssignmentErrorCode != (int)StreamsPartitionAssignor.Error.NONE)
                {
                    log.LogDebug($"Encountered assignment error during partition assignment: {streamThread.AssignmentErrorCode}. Skipping task initialization");
                }
                else
                {
                    log.LogDebug("Creating tasks based on assignment.");
                    taskManager.createTasks(assignedPartitions);
                }
            }
            catch (Exception t)
            {
                log.LogError(
                    "Error caught during partition assignment, " +
                        "will abort the current process and re-throw at the end of rebalance", t);
                streamThread.SetRebalanceException(t);
            }
            finally
            {
                log.LogInformation(
                    $"partition assignment took {clock.GetCurrentInstant().ToUnixTimeMilliseconds() - start} ms.\n" +
                    $"\tcurrent active tasks: {taskManager.activeTaskIds().ToJoinedString()}\n" +
                    $"\tcurrent standby tasks: {taskManager.StandbyTaskIds().ToJoinedString()}\n" +
                    $"\tprevious active tasks: {taskManager.prevActiveTaskIds().ToJoinedString()}\n");
            }
        }

        public void OnPartitionsRevoked(IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> revokedPartitions)
        {
            var assignment = consumer?.Assignment ?? new List<TopicPartition>();

            log.LogDebug(
                $"at state {streamThread.State}: partitions {assignment.ToJoinedString()} revoked at the beginning of consumer rebalance.\n" +
                $"\tcurrent assigned active tasks:  {taskManager.activeTaskIds().ToJoinedString()}\n" +
                $"\tcurrent assigned standby tasks: {taskManager.StandbyTaskIds().ToJoinedString()}\n");

            if (streamThread.State.SetState(KafkaStreamThreadStates.PARTITIONS_REVOKED))
            {
                long start = clock.GetCurrentInstant().ToUnixTimeMilliseconds();
                try
                {
                    // suspend active tasks
                    if (streamThread.AssignmentErrorCode == (int)StreamsPartitionAssignor.Error.VERSION_PROBING)
                    {
                        streamThread.AssignmentErrorCode = (int)StreamsPartitionAssignor.Error.NONE;
                    }
                    else
                    {
                        taskManager.SuspendTasksAndState();
                    }
                }
                catch (Exception t)
                {
                    log.LogError(
                        "Error caught during partition revocation, " +
                        $"will abort the current process and re-throw at the end of rebalance: {t}");

                    streamThread.SetRebalanceException(t);
                }
                finally
                {
                    streamThread.ClearStandbyRecords();

                    log.LogInformation(
                        $"partition revocation took {clock.GetCurrentInstant().ToUnixTimeMilliseconds() - start} ms.\n" +
                        $"\tsuspended active tasks: {taskManager.SuspendedActiveTaskIds().ToJoinedString()}\n" +
                        $"\tsuspended standby tasks: {taskManager.SuspendedStandbyTaskIds().ToJoinedString()}");
                }
            }
        }
    }
}
