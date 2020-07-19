using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Kafka.Streams.Processors.Interfaces;
using Kafka.Streams.Tasks;
using Kafka.Streams.Threads.Stream;
using Microsoft.Extensions.Logging;

namespace Kafka.Streams.Processors.Internals
{
    public class StreamsRebalanceListener : IConsumerRebalanceListener
    {
        private readonly KafkaStreamsContext context;
        private readonly ITaskManager taskManager;
        private readonly StreamThread streamThread;
        private readonly ILogger<StreamsRebalanceListener> log;

        public StreamsRebalanceListener(
            KafkaStreamsContext context,
            ITaskManager taskManager,
            StreamThread streamThread)
        {
            this.context = context ?? throw new ArgumentNullException(nameof(context));
            this.taskManager = taskManager ?? throw new ArgumentNullException(nameof(taskManager));
            this.streamThread = streamThread ?? throw new ArgumentNullException(nameof(streamThread));
            this.log = this.context.CreateLogger<StreamsRebalanceListener>();
        }

        public void OnPartitionsAssigned(IConsumer<byte[], byte[]>? consumer, List<TopicPartition> assignedPartitions)
        {
            if (consumer == null)
            {
                return;
            }

            this.log.LogDebug(
                $"at state {this.streamThread.State}: partitions {assignedPartitions.ToJoinedString()} assigned at the end of consumer rebalance.\n" +
                $"\tcurrent suspended active tasks: {this.taskManager.SuspendedActiveTaskIds().ToJoinedString()}\n" +
                $"\tcurrent suspended standby tasks: {this.taskManager.SuspendedStandbyTaskIds().ToJoinedString()}\n");

            if (this.streamThread.AssignmentErrorCode == (int)StreamsPartitionAssignor.Error.INCOMPLETE_SOURCE_TOPIC_METADATA)
            {
                this.log.LogError($"Received error code {this.streamThread.AssignmentErrorCode} - shutdown");
                this.streamThread.Shutdown();

                return;
            }

            var start = this.context.Clock.NowAsEpochMilliseconds;
            try
            {
                if (!this.streamThread.State.SetState(StreamThreadStates.PARTITIONS_ASSIGNED))
                {
                    this.log.LogDebug($"Skipping task creation in rebalance because we are already in {this.streamThread.State} state.");
                }
                else if (this.streamThread.AssignmentErrorCode != (int)StreamsPartitionAssignor.Error.NONE)
                {
                    this.log.LogDebug($"Encountered assignment error during partition assignment: {this.streamThread.AssignmentErrorCode}. Skipping task initialization");
                }
                else
                {
                    this.log.LogDebug("Creating tasks based on assignment.");
                    this.taskManager.SetAssignmentMetadata(new Dictionary<TaskId, HashSet<TopicPartition>>(), new Dictionary<TaskId, HashSet<TopicPartition>>());
                    this.taskManager.CreateTasks(assignedPartitions);
                }
            }
            catch (Exception t)
            {
                this.log.LogError(
                    "Error caught during partition assignment, " +
                        "will abort the current process and re-throw at the end of rebalance", t);
                this.streamThread.SetRebalanceException(t);
            }
            finally
            {
                this.log.LogInformation(
                    $"partition assignment took {this.context.Clock.NowAsEpochMilliseconds - start} ms.\n" +
                    $"\tcurrent active tasks: {this.taskManager.ActiveTaskIds().ToJoinedString()}\n" +
                    $"\tcurrent standby tasks: {this.taskManager.StandbyTaskIds().ToJoinedString()}\n" +
                    $"\tprevious active tasks: {this.taskManager.PrevActiveTaskIds().ToJoinedString()}\n");
            }
        }

        public void OnPartitionsRevoked(IConsumer<byte[], byte[]>? consumer, List<TopicPartitionOffset> revokedPartitions)
        {
            if (consumer == null)
            {
                return;
            }

            var assignment = consumer?.Assignment ?? new List<TopicPartition>();

            this.log.LogDebug(
                $"at state {this.streamThread.State}: partitions {assignment.ToJoinedString()} revoked at the beginning of consumer rebalance.\n" +
                $"\tcurrent assigned active tasks:  {this.taskManager.ActiveTaskIds().ToJoinedString()}\n" +
                $"\tcurrent assigned standby tasks: {this.taskManager.StandbyTaskIds().ToJoinedString()}\n");

            if (this.streamThread.State.SetState(StreamThreadStates.PARTITIONS_REVOKED))
            {
                var start = this.context.Clock.NowAsEpochMilliseconds;
                try
                {
                    // suspend active tasks
                    if (this.streamThread.AssignmentErrorCode == (int)StreamsPartitionAssignor.Error.VERSION_PROBING)
                    {
                        this.streamThread.AssignmentErrorCode = (int)StreamsPartitionAssignor.Error.NONE;
                    }
                    else
                    {
                        this.taskManager.SuspendTasksAndState();
                    }
                }
                catch (Exception t)
                {
                    this.log.LogError(
                        "Error caught during partition revocation, " +
                        $"will abort the current process and re-throw at the end of rebalance: {t}");

                    this.streamThread.SetRebalanceException(t);
                }
                finally
                {
                    this.streamThread.ClearStandbyRecords();

                    this.log.LogInformation(
                        $"partition revocation took {this.context.Clock.NowAsEpochMilliseconds - start} ms.\n" +
                        $"\tsuspended active tasks: {this.taskManager.SuspendedActiveTaskIds().ToJoinedString()}\n" +
                        $"\tsuspended standby tasks: {this.taskManager.SuspendedStandbyTaskIds().ToJoinedString()}");
                }
            }
        }
    }
}
